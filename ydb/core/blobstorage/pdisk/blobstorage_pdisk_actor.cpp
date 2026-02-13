#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_factory.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_thread.h"
#include "blobstorage_pdisk_tools.h"
#include "blobstorage_pdisk_util_cputimer.h"
#include "blobstorage_pdisk_writer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/html.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/util/random.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/schlab/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/string/split.h>
#include <util/system/sanitizers.h>
#include <util/generic/variant.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

void CreatePDiskActor(TExecutorThread& executorThread,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TIntrusivePtr<TPDiskConfig> &cfg,
        const NPDisk::TMainKey &mainKey,
        ui32 pDiskID, ui32 poolId, ui32 nodeId) {
    TActorId actorId = executorThread.RegisterActor(CreatePDisk(cfg, mainKey, cfg->MetadataOnly
        ? MakeIntrusive<NMonitoring::TDynamicCounters>() : counters), TMailboxType::ReadAsFilled, poolId);

    TActorId pDiskServiceId = MakeBlobStoragePDiskID(nodeId, pDiskID);

    executorThread.ActorSystem->RegisterLocalService(pDiskServiceId, actorId);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Actor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDiskActor : public TActorBootstrapped<TPDiskActor> {
    using TInitQueueItem = std::variant<
        NPDisk::TEvYardInit::TPtr,
        NPDisk::TEvShredPDisk::TPtr,
        NPDisk::TEvYardResize::TPtr,
        NPDisk::TEvChangeExpectedSlotCount::TPtr
    >;

    TString StateErrorReason;
    TIntrusivePtr<TPDiskConfig> Cfg;
    NPDisk::TMainKey MainKey;
    TList<TInitQueueItem> InitQueue;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> PDiskCounters;
    TIntrusivePtr<TPDisk> PDisk;
    bool IsMagicAlreadyChecked = false;

    THolder<TThread> FormattingThread;
    bool IsFormattingNow = false;
    std::function<void(bool, TString&)> PendingRestartResponse;

    TActorId NodeWhiteboardServiceId;
    TActorId NodeWardenServiceId;

    ui32 NextRestartRequestCookie = 0;

    THolder<IEventHandle> ControledStartResult;

    std::shared_ptr<TPDiskCtx> PCtx;

    class TWhiteboardFlag {
    private:
        class TSource {
        private:
            const TLightBase& Light;
            const ui64 ReportPeriodMs = 15000;
            const ui64 GreenRatio = 5;
            const ui64 YellowRatio = 25;
            const ui64 OrangeRatio = 75;

            ui64 LastCount = 0;
            ui64 LastRedMs = 0;
            ui64 LastGreenMs = 0;
            ui64 LastRedMsPs = 0;
            NKikimrWhiteboard::EFlag CurrentFlag = NKikimrWhiteboard::Green;
        public:
            explicit TSource(const TLightBase& light)
                : Light(light)
            {}

            ui64 GetRedMsPs() {
                return LastRedMsPs;
            }

            NKikimrWhiteboard::EFlag GetFlag() {
                ui64 count = Light.GetCount();
                ui64 redMs = Light.GetRedMs();
                ui64 greenMs = Light.GetGreenMs();

                // Init on first pass
                if (LastRedMs == 0 && LastGreenMs == 0) {
                    SaveLast(count, redMs, greenMs);
                }

                // Recalculate new flag value if needed
                if (greenMs + redMs > LastGreenMs + LastRedMs + ReportPeriodMs) {
                    ui64 ratio = (redMs - LastRedMs) * 100 / (redMs - LastRedMs + greenMs - LastGreenMs);
                    LastRedMsPs = (1000ull * (redMs - LastRedMs)) / (redMs - LastRedMs + greenMs - LastGreenMs);
                    if (ratio < GreenRatio) {
                        CurrentFlag = NKikimrWhiteboard::Green;
                    } else if (ratio < YellowRatio) {
                        CurrentFlag = NKikimrWhiteboard::Yellow;
                    } else if (ratio < OrangeRatio) {
                        CurrentFlag = NKikimrWhiteboard::Orange;
                    } else {
                        CurrentFlag = NKikimrWhiteboard::Red;
                    }
                    SaveLast(count, redMs, greenMs);
                }

                return CurrentFlag;
            }
        private:
            void SaveLast(ui64 count, ui64 redMs, ui64 greenMs) {
                LastCount = count;
                LastRedMs = redMs;
                LastGreenMs = greenMs;
            }
        };
    private:
        TVector<TSource> Sources;
        NKikimrWhiteboard::EFlag LastFlag = NKikimrWhiteboard::Grey;
    public:
        void AddSource(const TLightBase& light) {
            Sources.emplace_back(light);
        }

        void RemoveSources() {
            Sources.clear();
        }

        ui64 GetRedMsPs() {
            ui64 redMsPs = 0;
            for (TSource& source : Sources) {
                redMsPs = Max(redMsPs, source.GetRedMsPs());
            }
            return redMsPs;
        }

        void Update(bool& resendRequired) {
            NKikimrWhiteboard::EFlag flag = NKikimrWhiteboard::Green;
            for (TSource& source : Sources) {
                flag = Max(flag, source.GetFlag());
            }
            if (LastFlag != flag) {
                LastFlag = flag;
                resendRequired = true;
            }
        }

        NKikimrWhiteboard::EFlag Get() const {
            return LastFlag;
        }

        void Render(IOutputStream& os) const {
            switch (LastFlag) {
            case NKikimrWhiteboard::Grey:
                break;
            default:
                THtmlLightSignalRenderer(LastFlag, NKikimrWhiteboard::EFlag_Name(LastFlag)).Output(os);
                break;
            }
        }
    };

    TWhiteboardFlag RealtimeFlag;
    TWhiteboardFlag DeviceFlag;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PDISK_ACTOR;
    }

    TPDiskActor(const TIntrusivePtr<TPDiskConfig>& cfg, const NPDisk::TMainKey& mainKey,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Cfg(cfg)
        , MainKey(mainKey)
        , PDiskCounters(GetServiceCounters(counters, "pdisks")
                ->GetSubgroup("pdisk", Sprintf("%09" PRIu32, (ui32)cfg->PDiskId))
                ->GetSubgroup("media", to_lower(cfg->PDiskCategory.TypeStrShort())))
    {
        Y_VERIFY(MainKey.IsInitialized);
    }

    ~TPDiskActor() {
        if (FormattingThread) {
            FormattingThread->Join();
        }
        SecureWipeBuffer((ui8*)MainKey.Keys.data(), sizeof(NPDisk::TKey) * MainKey.Keys.size());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor handlers
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bootstrap state
    void Bootstrap(const TActorContext &ctx) {
        PCtx = std::make_shared<TPDiskCtx>(ctx.ActorSystem(), Cfg->PDiskId, SelfId());

        auto mon = AppData()->Mon;
        if (mon && !Cfg->MetadataOnly) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            NMonitoring::TIndexMonPage *pdisksMonPage = actorsMonPage->RegisterIndexPage("pdisks", "PDisks");

            TString path = Sprintf("pdisk%09" PRIu32, (ui32)Cfg->PDiskId);
            TString name = Sprintf("PDisk%09" PRIu32, (ui32)Cfg->PDiskId);
            mon->RegisterActorPage(pdisksMonPage, path, name, false, ctx.ActorSystem(),
                SelfId());
        }
        NodeWhiteboardServiceId = Cfg->MetadataOnly
            ? TActorId()
            : NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        NodeWardenServiceId = MakeBlobStorageNodeWardenID(SelfId().NodeId());

        Schedule(TDuration::MilliSeconds(Cfg->StatisticsUpdateIntervalMs), new TEvents::TEvWakeup());

        StartPDiskThread();
    }

    void StartPDiskThread() {
        PDisk = new TPDisk(PCtx, Cfg, PDiskCounters);

        RealtimeFlag.RemoveSources();
        DeviceFlag.RemoveSources();
        DeviceFlag.AddSource(PDisk->Mon.L6);

        bool isOk = PDisk->Initialize();

        if (!MainKey) {
            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "MainKey is invalid, ErrorReason# " << MainKey.ErrorReason;
            InitError(str.Str());
            P_LOG(PRI_CRIT, BPD01, str.Str());
        } else if (!isOk) {
            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "bootstrapped to the StateError, reason# " << PDisk->ErrorStr
                << " Can not be initialized";
            InitError(str.Str());
            str << " Config: " << Cfg->ToString();
            P_LOG(PRI_CRIT, BPD01, str.Str());
        } else {
            PDisk->InitiateReadSysLog(SelfId());
            StateErrorReason =
                "PDisk is in StateInit, wait for PDisk to read sys log. Did you ckeck EvYardInit result? Marker# BSY09";
            Become(&TThis::StateInit);
        }

        *PDisk->Mon.PDiskCount = 1;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Init state
    void InitError(const TString &errorReason, bool allowMetadataHandling = false) {
        Become(&TThis::StateError);
        for (TList<TInitQueueItem>::iterator it = InitQueue.begin(); it != InitQueue.end(); ++it) {
            std::visit([this, &errorReason](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, NPDisk::TEvShredPDisk::TPtr>) {
                    Send(ev->Sender, new NPDisk::TEvShredPDiskResult(NKikimrProto::CORRUPTED, ev->Get()->ShredGeneration, errorReason), 0, ev->Cookie);
                    if (PDisk) {
                        PDisk->Mon.ShredPDisk.CountResponse();
                    }
                } else if constexpr (std::is_same_v<T, NPDisk::TEvYardInit::TPtr>) {
                    Send(ev->Sender, new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, errorReason));
                    if (PDisk) {
                        PDisk->Mon.YardInit.CountResponse();
                    }
                } else if constexpr (std::is_same_v<T, NPDisk::TEvYardResize::TPtr>) {
                    Send(ev->Sender, new NPDisk::TEvYardResizeResult(NKikimrProto::CORRUPTED, {}, errorReason));
                    if (PDisk) {
                        PDisk->Mon.YardResize.CountResponse();
                    }
                } else if constexpr (std::is_same_v<T, NPDisk::TEvChangeExpectedSlotCount::TPtr>) {
                    Send(ev->Sender, new NPDisk::TEvChangeExpectedSlotCountResult(NKikimrProto::CORRUPTED, errorReason));
                    if (PDisk) {
                        PDisk->Mon.ChangeExpectedSlotCount.CountResponse();
                    }
                }
            }, *it);
        }
        InitQueue.clear();
        TStringStream str;
        str << "PDisk is in StateError, reason# " << errorReason;
        StateErrorReason = str.Str();
        if (PDisk) {
            PDisk->ErrorStr = StateErrorReason;
            if (allowMetadataHandling) {
                NeedToStopOnPoison = true;
            } else {
                PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TStopDevice>());
            }
        }

        if (ControledStartResult) {
            auto *ev = ControledStartResult->Get<TEvYardControlResult>();
            ev->Status = NKikimrProto::CORRUPTED;
            ev->ErrorReason = StateErrorReason;
            TlsActivationContext->Send(ControledStartResult.Release());
        }

        StartHandlingMetadata(!allowMetadataHandling);
    }

    void InitHandle(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream outStr;
        outStr.Reserve(512 << 10);
        TStringStream deviceFlagStr;
        DeviceFlag.Render(deviceFlagStr);
        TStringStream realtimeFlagStr;
        RealtimeFlag.Render(realtimeFlagStr);
        TStringStream fairSchedulerStr;
        THolder<THttpInfo> req(PDisk->ReqCreator.CreateFromArgs<THttpInfo>(SelfId(), ev->Sender, outStr,
                    deviceFlagStr.Str(), realtimeFlagStr.Str(), fairSchedulerStr.Str(), PDisk->ErrorStr, false));
        if (!IsFormattingNow) {
            PDisk->InputRequest(req.Release());
        } else {
            PDisk->HttpInfo(*req); // Sends TEvHttpInfoResult inside
        }
    }

    void InitHandle(TEvPDiskFormattingFinished::TPtr &ev) {
        FormattingThread->Join();
        IsFormattingNow = false;
        if (ev->Get()->IsSucceed) {
            PDiskGuid.emplace(Cfg->PDiskGuid);
            StartPDiskThread();
            P_LOG(PRI_WARN, BSP01, "Device formatting done");
        } else {
            RealtimeFlag.RemoveSources();
            DeviceFlag.RemoveSources();
            PDisk.Reset(new TPDisk(PCtx, Cfg, PDiskCounters));
            PDisk->Initialize();
            Y_VERIFY_S(PDisk->PDiskThread.Running(), PCtx->PDiskLogPrefix);

            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDiskCannotBeFormated;

            PDisk->ErrorStr = ToString("Can not be formated! Reason# ") + ev->Get()->ErrorStr;

            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "Can not be formated! Reason# " << ev->Get()->ErrorStr
                << " Switching to StateError. Config: " << Cfg->ToString();
            P_LOG(PRI_CRIT, BPD01, str.Str());
            InitError(str.Str());
        }
    }

    void CheckMagicSector(ui8 *magicData, ui32 magicDataSize) {
        bool isFormatMagicValid = PDisk->IsFormatMagicValid(magicData, magicDataSize, MainKey);
        if (isFormatMagicValid) {
            auto format = PDisk->CheckMetadataFormatSector(
                magicData,
                magicDataSize,
                MainKey,
                PCtx->PDiskLogPrefix,
                Cfg->EnableFormatEncryption);
            PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TPushUnformattedMetadataSector>(format,
                !Cfg->MetadataOnly));
            if (Cfg->MetadataOnly) {
                InitError("MetadataOnly is set to true, not formatting PDisk", true);
            } else {
                IsMagicAlreadyChecked = true;
                IsFormattingNow = true;
            }
        } else {
            SecureWipeBuffer(reinterpret_cast<ui8*>(MainKey.Keys.data()), sizeof(NPDisk::TKey) * MainKey.Keys.size());
            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorPDiskCannotBeInitialised;
            if (!IsMagicAlreadyChecked) {
                PDisk->ErrorStr = "Format is incomplete. Magic sector is not present on disk. Maybe wrong PDiskKey";
            } else {
                PDisk->ErrorStr = "Format is incomplete. Magic sector is present and new format was written";
            }
            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "Can not be initialized! " << PDisk->ErrorStr;
            for (ui32 i = 0; i < Cfg->HashedMainKey.size(); ++i) {
                str << " Hash(NewMainKey[" << i << "])# " << Cfg->HashedMainKey[i];
            }
            InitError(str.Str());
            str << " Config: " << Cfg->ToString();
            P_LOG(PRI_CRIT, BPD01, str.Str());
        }
    }

    void InitHandle(TEvPDiskMetadataLoaded::TPtr ev) {
        // Stop PDiskThread but use PDisk object for creation of http pages
        PDisk->Stop();
        *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingDeviceFormattingAndTrimming;
        PDisk->ErrorStr = "Magic sector is present on disk, now going to format device";
        P_LOG(PRI_WARN, BSP01, PDisk->ErrorStr);

        // Is used to pass parameters into formatting thread, because TThread can pass only void*
        using TCookieType = std::tuple<TIntrusivePtr<TPDiskConfig>, NPDisk::TKey, TActorSystem*, TActorId, std::optional<TRcBuf>>;
        FormattingThread.Reset(new TThread(
                [] (void *cookie) -> void* {
                    auto params = static_cast<TCookieType*>(cookie);
                    auto [cfg, mainKey, actorSystem, pDiskActor, metadata] = std::move(*params);
                    delete params;

                    if (cfg->ReadOnly) {
                        TString readOnlyError = "PDisk is in read-only mode";
                        STLOGX(*actorSystem, PRI_ERROR, BS_PDISK, BSP01, "Formatting error", (What, readOnlyError));
                        actorSystem->Send(pDiskActor, new TEvPDiskFormattingFinished(false, readOnlyError));
                        return nullptr;
                    }

                    NPDisk::TKey chunkKey;
                    NPDisk::TKey logKey;
                    NPDisk::TKey sysLogKey;
                    SafeEntropyPoolRead(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
                    SafeEntropyPoolRead(&logKey, sizeof(NKikimr::NPDisk::TKey));
                    SafeEntropyPoolRead(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

                    try {
                        TFormatOptions options;
                        options.TrimEntireDevice = cfg->FeatureFlags.GetTrimEntireDeviceOnStartup();
                        options.SectorMap = cfg->SectorMap;
                        options.EnableSmallDiskOptimization = cfg->FeatureFlags.GetEnableSmallDiskOptimization();
                        options.Metadata = metadata;
                        options.PlainDataChunks = cfg->PlainDataChunks;
                        options.EnableFormatEncryption = cfg->EnableFormatEncryption;
                        options.EnableSectorEncryption = cfg->EnableSectorEncryption;

                        try {
                            FormatPDisk(cfg->GetDevicePath(), 0, cfg->SectorSize, cfg->ChunkSize,
                                cfg->PDiskGuid, chunkKey, logKey, sysLogKey, mainKey, TString(),
                                options);
                        } catch (NPDisk::TPDiskFormatBigChunkException) {
                            FormatPDisk(cfg->GetDevicePath(), 0, cfg->SectorSize, NPDisk::SmallDiskMaximumChunkSize,
                                cfg->PDiskGuid, chunkKey, logKey, sysLogKey, mainKey, TString(),
                                options);
                        }
                        actorSystem->Send(pDiskActor, new TEvPDiskFormattingFinished(true, ""));
                    } catch (yexception ex) {
                        STLOGX(*actorSystem, PRI_ERROR, BS_PDISK, BSP01, "Formatting error", (What, ex.what()));
                        actorSystem->Send(pDiskActor, new TEvPDiskFormattingFinished(false, ex.what()));
                    }
                    return nullptr;
                },
                new TCookieType(Cfg, MainKey.Keys.back(), TlsActivationContext->ActorSystem(), SelfId(), std::move(ev->Get()->Metadata))));

        FormattingThread->Start();
    }

    void ReencryptDiskFormat(const TDiskFormat& format, const NPDisk::TKey& newMainKey) {
        IsFormattingNow = true;
        // Stop PDiskThread but use PDisk object for creation of http pages
        PDisk->Stop();
        *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingReencryptingFormat;
        PDisk->ErrorStr = "Format sectors are encrypted with an old PDisk key, reencryption started";
        P_LOG(PRI_WARN, BSP01, PDisk->ErrorStr);

        // Is used to pass parameters into formatting thread, because TThread can pass only void*
        using TCookieType = std::tuple<TDiskFormat, NPDisk::TKey, TIntrusivePtr<TPDiskConfig>, std::shared_ptr<TPDiskCtx>>;
        FormattingThread.Reset(new TThread(
            [] (void *cookie) -> void* {
                std::unique_ptr<TCookieType> params(static_cast<TCookieType*>(cookie));
                TDiskFormat format = std::get<0>(*params);
                NPDisk::TKey mainKey = std::get<1>(*params);
                TIntrusivePtr<TPDiskConfig> cfg = std::get<2>(*params);
                const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);
                std::shared_ptr<TPDiskCtx> pCtx = std::get<3>(*params);

                if (cfg->ReadOnly) {
                    TString readOnlyError = "PDisk is in read-only mode";
                    STLOGX(*pCtx->ActorSystem, PRI_ERROR, BS_PDISK, BSP01, "Formatting error", (What, readOnlyError));
                    pCtx->ActorSystem->Send(pCtx->PDiskActor, new TEvPDiskFormattingFinished(false, readOnlyError));
                    return nullptr;
                }

                THolder<NPDisk::TPDisk> pDisk(new NPDisk::TPDisk(pCtx, cfg, counters));

                pDisk->Initialize();

                if (!pDisk->BlockDevice->IsGood()) {
                    ythrow yexception() << "Failed to initialize temporal PDisk for format rewriting, info# " << pDisk->BlockDevice->DebugInfo();
                }

                try {
                    pDisk->WriteApplyFormatRecord(format, mainKey);
                    pCtx->ActorSystem->Send(pCtx->PDiskActor, new TEvFormatReencryptionFinish(true, ""));
                } catch (yexception ex) {
                    STLOGX(*pCtx->ActorSystem, PRI_ERROR, BS_PDISK, BPD01, "Reencryption error", (What, ex.what()));
                    pCtx->ActorSystem->Send(pCtx->PDiskActor, new TEvFormatReencryptionFinish(false, ex.what()));
                }
                return nullptr;
            },
            new TCookieType(format, newMainKey, PDisk->Cfg, PCtx)
        ));
        FormattingThread->Start();
    }

    void InitHandle(TEvFormatReencryptionFinish::TPtr &ev) {
        FormattingThread->Join();
        IsFormattingNow = false;
        if (ev->Get()->Success) {
            StartPDiskThread();
            P_LOG(PRI_WARN, BSP01, "Format chunks reencryption finished");
        } else {
            RealtimeFlag.RemoveSources();
            DeviceFlag.RemoveSources();
            PDisk.Reset(new TPDisk(PCtx, Cfg, PDiskCounters));
            PDisk->Initialize();
            Y_VERIFY_S(PDisk->PDiskThread.Running(), PCtx->PDiskLogPrefix);

            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDiskCannotBeFormated;

            PDisk->ErrorStr = ToString("Format chunks cannot be reencrypted! Reason# ") + ev->Get()->ErrorReason;

            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "Format chunks cannot be reencrypted! Reason# " << ev->Get()->ErrorReason
                << " Switching to StateError. Config: " << Cfg->ToString();
            P_LOG(PRI_CRIT, BPD01, str.Str());
            InitError(str.Str());
        }
    }

    void InitHandle(TEvReadFormatResult::TPtr &ev) {
        ui8 *formatSectors = ev->Get()->FormatSectors.Get();
        ui32 formatSectorsSize = ev->Get()->FormatSectorsSize;
        NSan::CheckMemIsInitialized(formatSectors, formatSectorsSize);
        TCheckDiskFormatResult res = PDisk->ReadChunk0Format(formatSectors, MainKey);
        if (!res.IsFormatPresent) {
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingFormatMagicChecking;
            PDisk->ErrorStr = "Format chunks are not present on disk or corrupted, now checking for proper magic sector on disk";
            CheckMagicSector(formatSectors, formatSectorsSize);
        } else {
            PDiskGuid.emplace(PDisk->Format.Guid);
            if (res.IsReencryptionRequired) {
                // Format reencryption required
                ReencryptDiskFormat(PDisk->Format, MainKey.Keys.back());
                // We still need main key after restart
                // SecureWipeBuffer((ui8*)MainKey.data(), sizeof(NPDisk::TKey) * MainKey.size());
            } else {
                // Format is read OK
                SecureWipeBuffer((ui8*)MainKey.Keys.data(), sizeof(NPDisk::TKey) * MainKey.Keys.size());
                P_LOG(PRI_NOTICE, BSP01, "Successfully read format record", (Format, PDisk->Format.ToString()));
                TString info;
                if (!PDisk->CheckGuid(&info)) {
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
                    *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialFormatReadDueToGuid;
                    PDisk->ErrorStr = TStringBuilder() << "Can't start due to a guid error " << info;
                    TStringStream str;
                    str << PCtx->PDiskLogPrefix << PDisk->ErrorStr;
                    P_LOG(PRI_ERROR, BSP01, str.Str());
                    InitError(str.Str());
                } else if (!PDisk->CheckFormatComplete()) {
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
                    *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialFormatReadIncompleteFormat;
                    PDisk->ErrorStr = "Can't start due to incomplete format!";
                    TStringStream str;
                    str << PCtx->PDiskLogPrefix << PDisk->ErrorStr << " "
                        << "Please, do not turn off your server or remove your storage device while formatting. "
                        << "We are sure you did this or something even more creative, like killing the formatter.";
                    P_LOG(PRI_ERROR, BSP01, str.Str());
                    InitError(str.Str());
                } else {
                    // PDisk GUID is OK and format is complete
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialSysLogRead;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingSysLogRead;
                    PDisk->ReadSysLog(SelfId());
                }
            }
        }
    }

    void InitHandle(NPDisk::TEvReadLogResult::TPtr &ev) {
        auto *request = PDisk->ReqCreator.CreateFromEv<TLogReadResultProcess>(ev, SelfId());
        PDisk->InputRequest(request);
    }

    void InitHandle(NPDisk::TEvLogInitResult::TPtr &ev) {
        const NPDisk::TEvLogInitResult &evLogInitResult = *ev->Get();
        PDisk->ErrorStr = evLogInitResult.ErrorStr;
        if (evLogInitResult.IsInitializedGood) {
            InitSuccess();
        } else {
            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << " Can't start due to a log processing error! ErrorStr# \"" << evLogInitResult.ErrorStr << "\"";
            P_LOG(PRI_ERROR, BSP01, str.Str());
            InitError(str.Str());
        }
    }

    void InitSuccess() {
        Become(&TThis::StateOnline);
        for (TList<TInitQueueItem>::iterator it = InitQueue.begin(); it != InitQueue.end(); ++it) {
            std::visit([this](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, NPDisk::TEvShredPDisk::TPtr>) {
                    auto* request = PDisk->ReqCreator.CreateFromEv<NPDisk::TShredPDisk>(*ev->Get(), ev->Sender);
                    request->Cookie = ev->Cookie;
                    PDisk->InputRequest(request);
                } else if constexpr (std::is_same_v<T, NPDisk::TEvYardInit::TPtr>) {
                    auto* request = PDisk->ReqCreator.CreateFromEv<TYardInit>(*ev->Get(), ev->Sender);
                    PDisk->InputRequest(request);
                } else if constexpr (std::is_same_v<T, NPDisk::TEvYardResize::TPtr>) {
                    auto* request = PDisk->ReqCreator.CreateFromEv<TYardResize>(*ev->Get(), ev->Sender);
                    PDisk->InputRequest(request);
                } else if constexpr (std::is_same_v<T, NPDisk::TEvChangeExpectedSlotCount::TPtr>) {
                    auto* request = PDisk->ReqCreator.CreateFromEv<TChangeExpectedSlotCount>(*ev->Get(), ev->Sender);
                    PDisk->InputRequest(request);
                }
            }, *it);
        }
        InitQueue.clear();
        if (ControledStartResult) {
            TlsActivationContext->Send(ControledStartResult.Release());
        }
        StartHandlingMetadata(false);
    }

    void InitHandle(NPDisk::TEvYardInit::TPtr &ev) {
        InitQueue.emplace_back(ev);
    }

    void OnPDiskStop(TActorId &sender, void *cookie) {
        if (PDisk) {
            PDisk->Stop();
            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::Stopped;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Stopped;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::StoppedByYardControl;
        }
        InitError("Received TEvYardControl::PDiskStop");
        Send(sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, cookie, {}));
    }

    void InitHandle(NPDisk::TEvYardControl::TPtr &ev) {
        const NPDisk::TEvYardControl &evControl = *ev->Get();
        switch (evControl.Action) {
        case TEvYardControl::PDiskStart:
            ControledStartResult = MakeHolder<IEventHandle>(ev->Sender, SelfId(),
                    new TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
        break;
        case TEvYardControl::PDiskStop:
            OnPDiskStop(ev->Sender, evControl.Cookie);
            break;
        default:
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::CORRUPTED, evControl.Cookie,
                        "Unexpected control action for pdisk in StateInit"));
            PDisk->Mon.YardControl.CountResponse();
        break;
        }

    }

    void InitHandle(NPDisk::TEvSlay::TPtr &ev) {
        const NPDisk::TEvSlay &evSlay = *ev->Get();
        PDisk->Mon.YardSlay.CountRequest();
        TStringStream str;
        str << PCtx->PDiskLogPrefix << "is still initializing, please wait";
        Send(ev->Sender, new NPDisk::TEvSlayResult(NKikimrProto::NOTREADY, 0,
                    evSlay.VDiskId, evSlay.SlayOwnerRound, evSlay.PDiskId, evSlay.VSlotId, str.Str()));
        PDisk->Mon.YardSlay.CountResponse();
    }

    void InitHandle(NPDisk::TEvYardResize::TPtr &ev) {
        PDisk->Mon.YardResize.CountRequest();
        InitQueue.emplace_back(ev);
    }

    void InitHandle(NPDisk::TEvChangeExpectedSlotCount::TPtr &ev) {
        PDisk->Mon.ChangeExpectedSlotCount.CountRequest();
        InitQueue.emplace_back(ev);
    }

    void InitHandle(NPDisk::TEvShredPDisk::TPtr &ev) {
        InitQueue.emplace_back(ev);
    }

    void InitHandle(NPDisk::TEvPreShredCompactVDiskResult::TPtr &ev) {
        // Just ignore the event, can't pre-shred compact in this state.
        Y_UNUSED(ev);
    }

    void InitHandle(NPDisk::TEvShredVDiskResult::TPtr &ev) {
        // Just ignore the event, can't shred in this state.
        Y_UNUSED(ev);
    }

    void InitHandle(NPDisk::TEvContinueShred::TPtr &ev) {
        // Just ignore the event, can't shred in this state.
        Y_UNUSED(ev);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Error state

    void ErrorHandle(NPDisk::TEvYardInit::TPtr &ev) {
        PDisk->Mon.YardInit.CountRequest();
        Send(ev->Sender, new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, StateErrorReason));
        PDisk->Mon.YardInit.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvCheckSpace::TPtr &ev) {
        PDisk->Mon.CheckSpace.CountRequest();
        Send(ev->Sender, new NPDisk::TEvCheckSpaceResult(NKikimrProto::CORRUPTED, 0, 0, 0, 0, 0, 0u, StateErrorReason));
        PDisk->Mon.CheckSpace.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvLog::TPtr &ev) {
        const NPDisk::TEvLog &evLog = *ev->Get();
        TStringStream str;
        str << PCtx->PDiskLogPrefix;
        str << "TEvLog error because PDisk State# ";
        if (CurrentStateFunc() == &TPDiskActor::StateInit) {
            str << "Init, wait for PDisk to initialize. Did you ckeck EvYardInit result? Marker# BSY08";
        } else if (CurrentStateFunc() == &TPDiskActor::StateError) {
            str << "Error, there is a terminal internal error in PDisk. Did you check EvYardInit result? Marker# BSY07";
        } else {
            str << "Unknown, something went very wrong in PDisk. Marker# BSY06";
        }
        str << " StateErrorReason# " << StateErrorReason;
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, str.Str(), 0));
        result->Results.push_back(NPDisk::TEvLogResult::TRecord(evLog.Lsn, evLog.Cookie));
        PDisk->Mon.WriteLog.CountRequest(0);
        Send(ev->Sender, result.Release());
        PDisk->Mon.WriteLog.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvLogResult::TPtr &ev) {
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvMultiLog::TPtr &ev) {
        const NPDisk::TEvMultiLog &evMultiLog = *ev->Get();
        TStringStream str;
        str << PCtx->PDiskLogPrefix;
        str << "TEvBatchedLogs error because PDisk State# ";
        if (CurrentStateFunc() == &TPDiskActor::StateInit) {
            str << "Init, wait for PDisk to initialize. Did you ckeck EvYardInit result? Marker# BSY10";
        } else if (CurrentStateFunc() == &TPDiskActor::StateError) {
            str << "Error, there is a terminal internal error in PDisk. Did you check EvYardInit result? Marker# BSY11";
        } else {
            str << "Unknown, something went very wrong in PDisk. Marker# BSY12";
        }
        str << " StateErrorReason# " << StateErrorReason;
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, str.Str(), 0));
        for (auto &[log, _] : evMultiLog.Logs) {
            result->Results.push_back(NPDisk::TEvLogResult::TRecord(log->Lsn, log->Cookie));
        }
        PDisk->Mon.WriteLog.CountRequest(0);
        Send(ev->Sender, result.Release());
        PDisk->Mon.WriteLog.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvReadLog::TPtr &ev) {
        const NPDisk::TEvReadLog &evReadLog = *ev->Get();
        TStringStream str;
        str << PCtx->PDiskLogPrefix;
        str << "TEvReadLog error because PDisk State# ";
        if (CurrentStateFunc() == &TPDiskActor::StateInit) {
            str << "Init, wait for PDisk to initialize. Did you ckeck EvYardInit result? Marker# BSY05";
        } else if (CurrentStateFunc() == &TPDiskActor::StateError) {
            str << "Error, there is a terminal internal error in PDisk. Did you check EvYardInit result? Marker# BSY04";
        } else {
            str << "Unknown, something went very wrong in PDisk. Marker# BSY03";
        }
        THolder<NPDisk::TEvReadLogResult> result(new NPDisk::TEvReadLogResult(
            NKikimrProto::CORRUPTED, evReadLog.Position, evReadLog.Position, true, 0, str.Str(), evReadLog.Owner));
        PDisk->Mon.LogRead.CountRequest();
        Send(ev->Sender, result.Release());
        PDisk->Mon.LogRead.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChunkWriteRaw::TPtr ev) {
        Send(ev->Sender, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::CORRUPTED, StateErrorReason), 0, ev->Cookie);
    }

    void ErrorHandle(NPDisk::TEvChunkReadRaw::TPtr ev) {
        Send(ev->Sender, new NPDisk::TEvChunkReadRawResult(NKikimrProto::CORRUPTED, StateErrorReason), 0, ev->Cookie);
    }

    void ErrorHandle(NPDisk::TEvChunkWrite::TPtr &ev) {
        const NPDisk::TEvChunkWrite &evChunkWrite = *ev->Get();
        PDisk->Mon.GetWriteCounter(evChunkWrite.PriorityClass)->CountRequest(0);
        PDisk->Mon.GetWriteCounter(evChunkWrite.PriorityClass)->CountResponse();
        auto res = std::make_unique<NPDisk::TEvChunkWriteResult>(NKikimrProto::CORRUPTED,
            evChunkWrite.ChunkIdx, evChunkWrite.Cookie, 0, StateErrorReason);
        res->Orbit = std::move(ev->Get()->Orbit);
        Send(ev->Sender, res.release());
    }

    void ErrorHandle(NPDisk::TEvChunkRead::TPtr &ev) {
        const NPDisk::TEvChunkRead &evChunkRead = *ev->Get();
        PDisk->Mon.GetReadCounter(evChunkRead.PriorityClass)->CountRequest(0);
        THolder<NPDisk::TEvChunkReadResult> result = MakeHolder<NPDisk::TEvChunkReadResult>(NKikimrProto::CORRUPTED,
            evChunkRead.ChunkIdx, evChunkRead.Offset, evChunkRead.Cookie, 0, "PDisk is in error state");
        result->Data.SetDebugInfoGenerator(PDisk->DebugInfoGenerator);

        P_LOG(PRI_DEBUG, BSY02, "ReplyErrror", (Result, result->ToString()), (To, ev->Sender.LocalId()));
        Send(ev->Sender, result.Release());
        PDisk->Mon.GetReadCounter(evChunkRead.PriorityClass)->CountResponse();
    }

    void ErrorHandle(NPDisk::TEvHarakiri::TPtr &ev) {
        PDisk->Mon.Harakiri.CountRequest();
        Send(ev->Sender, new NPDisk::TEvHarakiriResult(NKikimrProto::CORRUPTED, 0, StateErrorReason));
        PDisk->Mon.Harakiri.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvSlay::TPtr &ev) {
        const NPDisk::TEvSlay &evSlay = *ev->Get();
        PDisk->Mon.YardSlay.CountRequest();
        TStringStream str;
        str << PCtx->PDiskLogPrefix << "is in error state.";
        Send(ev->Sender, new NPDisk::TEvSlayResult(NKikimrProto::CORRUPTED, 0,
                    evSlay.VDiskId, evSlay.SlayOwnerRound, evSlay.PDiskId, evSlay.VSlotId, str.Str()));
        PDisk->Mon.YardSlay.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvYardResize::TPtr &ev) {
        PDisk->Mon.YardResize.CountRequest();
        Send(ev->Sender, new NPDisk::TEvYardResizeResult(NKikimrProto::CORRUPTED, {}, StateErrorReason));
        PDisk->Mon.YardResize.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChangeExpectedSlotCount::TPtr &ev) {
        PDisk->Mon.ChangeExpectedSlotCount.CountRequest();
        Send(ev->Sender, new NPDisk::TEvChangeExpectedSlotCountResult(NKikimrProto::CORRUPTED, StateErrorReason));
        PDisk->Mon.ChangeExpectedSlotCount.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChunkReserve::TPtr &ev) {
        PDisk->Mon.ChunkReserve.CountRequest();
        Send(ev->Sender, new NPDisk::TEvChunkReserveResult(NKikimrProto::CORRUPTED, 0, StateErrorReason), 0, ev->Cookie);
        PDisk->Mon.ChunkReserve.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChunkForget::TPtr &ev) {
        PDisk->Mon.ChunkForget.CountRequest();
        Send(ev->Sender, new NPDisk::TEvChunkForgetResult(NKikimrProto::CORRUPTED, 0, StateErrorReason));
        PDisk->Mon.ChunkForget.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvYardControl::TPtr &ev) {
        const NPDisk::TEvYardControl &evControl = *ev->Get();
        Y_VERIFY_S(PDisk, PCtx->PDiskLogPrefix);

        PDisk->Mon.YardControl.CountRequest();

        switch (evControl.Action) {
        case TEvYardControl::PDiskStart:
        {
            auto *mainKey = static_cast<const NPDisk::TMainKey*>(evControl.Cookie);
            Y_VERIFY_S(mainKey, PCtx->PDiskLogPrefix);
            MainKey = *mainKey;
            StartPDiskThread();
            ControledStartResult = MakeHolder<IEventHandle>(ev->Sender, SelfId(),
                    new TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
            break;
        }
        default:
            // Only PDiskStart is allowed in StateError. PDiskStop is not allowed since PDisk in error state should already be stopped
            // or in the process of being stopped.
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::CORRUPTED, evControl.Cookie, StateErrorReason));
            PDisk->Mon.YardControl.CountResponse();
            break;
        }
    }

    void ErrorHandle(TEvReadFormatResult::TPtr &ev) {
        // Just ignore the event, disk is in error state.
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvAskForCutLog::TPtr &ev) {
        // Just ignore the event, can't send cut log in this state.
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvShredPDisk::TPtr &ev) {
        // Respond with error, can't shred in this state.
        Send(ev->Sender, new NPDisk::TEvShredPDiskResult(NKikimrProto::CORRUPTED, 0, StateErrorReason), 0, ev->Cookie);
    }

    void ErrorHandle(NPDisk::TEvPreShredCompactVDiskResult::TPtr &ev) {
        // Just ignore the event, can't pre-shred compact in this state.
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvShredVDiskResult::TPtr &ev) {
        // Just ignore the event, can't shred in this state.
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvContinueShred::TPtr &ev) {
        // Just ignore the event, can't shred in this state.
        Y_UNUSED(ev);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Online state

    void Handle(NPDisk::TEvYardInit::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TYardInit>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvCheckSpace::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TCheckSpace>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvLog::TPtr &ev) {
        double burstMs;
        TLogWrite* request = PDisk->ReqCreator.CreateLogWrite(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        request->Orbit = std::move(ev->Get()->Orbit);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvLogResult::TPtr &ev) {
        if (PDisk->ShredLogPaddingInFlight) {
            PDisk->ShredLogPaddingInFlight--;
        }
        if (PDisk->ContinueShredsInFlight == 0) {
            TEvContinueShred evCont;
            TContinueShred* request = PDisk->ReqCreator.CreateFromEv<TContinueShred>(evCont, ev->Sender);
            PDisk->ContinueShredsInFlight++;
            PDisk->InputRequest(request);
        }
    }

    void Handle(NPDisk::TEvMultiLog::TPtr &ev) {
        for (auto &[log, traceId] : ev->Get()->Logs) {
            double burstMs;
            TLogWrite* request = PDisk->ReqCreator.CreateLogWrite(*log, ev->Sender, burstMs, std::move(traceId));
            request->Orbit = std::move(log->Orbit);
            PDisk->InputRequest(request);
        }
    }

    void Handle(NPDisk::TEvReadLog::TPtr &ev) {
        P_LOG(PRI_DEBUG, BSY01, "Got TEvReadLog", (Event, ev->Get()->ToString()));
        double burstMs;
        auto* request = PDisk->ReqCreator.CreateFromEvPtr<TLogRead>(ev, &burstMs);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkWriteRaw::TPtr ev) {
        PDisk->InputRequest(PDisk->ReqCreator.CreateChunkWriteRaw(*ev));
    }

    void Handle(NPDisk::TEvChunkReadRaw::TPtr ev) {
        PDisk->InputRequest(PDisk->ReqCreator.CreateChunkReadRaw(*ev));
    }

    void Handle(NPDisk::TEvChunkWrite::TPtr &ev) {
        double burstMs;
        TChunkWrite* request = PDisk->ReqCreator.CreateChunkWrite(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        request->Orbit = std::move(ev->Get()->Orbit);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkRead::TPtr &ev) {
        double burstMs;
        TChunkRead* request = PDisk->ReqCreator.CreateChunkRead(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        request->DebugInfoGenerator = PDisk->DebugInfoGenerator;
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvHarakiri::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<THarakiri>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvSlay::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TSlay>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvYardResize::TPtr &ev) {
        PDisk->Mon.YardResize.CountRequest();
        TYardResize* request = PDisk->ReqCreator.CreateFromEv<TYardResize>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChangeExpectedSlotCount::TPtr &ev) {
        PDisk->Mon.YardResize.CountRequest();
        TChangeExpectedSlotCount* request = PDisk->ReqCreator.CreateFromEv<TChangeExpectedSlotCount>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkReserve::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkReserve>(*ev->Get(), ev->Sender, ev->Cookie);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkForget::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkForget>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkLock::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkUnlock::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkUnlock>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvYardControl::TPtr &ev) {
        const NPDisk::TEvYardControl &evControl = *ev->Get();
        switch (evControl.Action) {
        case TEvYardControl::Brake:
            InitError("Received TEvYardControl::Brake");
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
            break;
        case TEvYardControl::PDiskStop:
            OnPDiskStop(ev->Sender, evControl.Cookie);
            break;
        case TEvYardControl::GetPDiskPointer:
            Y_VERIFY_S(!evControl.Cookie, PCtx->PDiskLogPrefix);
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, PDisk.Get(), {}));
            break;
        case TEvYardControl::PDiskStart:
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, nullptr, {}));
            break;
        default:
            auto* request = PDisk->ReqCreator.CreateFromEv<TYardControl>(evControl, ev->Sender);
            PDisk->InputRequest(request);
            break;
        }
    }

    void Handle(NPDisk::TEvAskForCutLog::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEvPtr<TAskForCutLog>(ev);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvConfigureScheduler::TPtr &ev) {
        P_LOG(PRI_INFO, BSP01, "Got TEvConfigureScheduler", (Event, ev->Get()->ToString()));
        PDisk->Mon.YardConfigureScheduler.CountRequest();
        // Configure forseti scheduler weights
        auto* request = PDisk->ReqCreator.CreateFromEv<TConfigureScheduler>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    std::optional<ui64> PDiskGuid;
    std::deque<TAutoPtr<IEventHandle>> PendingMetadata;
    enum class EMetadataHandlingState {
        WAITING_FOR_STARTUP,
        PROCESSING,
        ERROR,
    } MetadataHandlingState = EMetadataHandlingState::WAITING_FOR_STARTUP;
    bool NeedToStopOnPoison = false;

    void DropMetadata() {
        for (auto& ev : std::exchange(PendingMetadata, {})) {
            switch (ev->GetTypeRewrite()) {
                case TEvReadMetadata::EventType:
                    Send(ev->Sender, new TEvReadMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
                    break;

                case TEvWriteMetadata::EventType:
                    Send(ev->Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
                    break;
            }
        }
    }

    void StartHandlingMetadata(bool error) {
        MetadataHandlingState = error
            ? EMetadataHandlingState::ERROR
            : EMetadataHandlingState::PROCESSING;
        for (auto& ev : std::exchange(PendingMetadata, {})) {
            Receive(ev);
        }
    }

    void Handle(TEvReadMetadata::TPtr& ev) {
        switch (MetadataHandlingState) {
            case EMetadataHandlingState::WAITING_FOR_STARTUP:
                PendingMetadata.emplace_back(ev.Release());
                break;

            case EMetadataHandlingState::PROCESSING:
                PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TReadMetadata>(ev->Sender, MainKey));
                break;

            case EMetadataHandlingState::ERROR:
                Send(ev->Sender, new TEvReadMetadataResult(EPDiskMetadataOutcome::ERROR, PDiskGuid), 0, ev->Cookie);
                break;
        }
    }

    void Handle(TEvWriteMetadata::TPtr& ev) {
        switch (MetadataHandlingState) {
            case EMetadataHandlingState::WAITING_FOR_STARTUP:
                PendingMetadata.emplace_back(ev.Release());
                break;

            case EMetadataHandlingState::PROCESSING:
                PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TWriteMetadata>(ev->Sender,
                    std::move(ev->Get()->Metadata), MainKey));
                break;

            case EMetadataHandlingState::ERROR:
                Send(ev->Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, PDiskGuid), 0, ev->Cookie);
                break;
        }
    }

    void Handle(NPDisk::TEvShredPDisk::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TShredPDisk>(*ev->Get(), ev->Sender);
        request->Cookie = ev->Cookie;
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvPreShredCompactVDiskResult::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TPreShredCompactVDiskResult>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvShredVDiskResult::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TShredVDiskResult>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvContinueShred::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TContinueShred>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // All states

    void PassAway() override {
        DropMetadata();
        TActorBootstrapped::PassAway();
    }

    void HandlePoison() {
        if (NeedToStopOnPoison && PDisk) {
            PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TStopDevice>());
        }
        RealtimeFlag.RemoveSources();
        DeviceFlag.RemoveSources();
        PDisk.Reset();
        PassAway();
        P_LOG(PRI_NOTICE, BSP01, "HandlePoison, PDiskThread stopped");
    }

    void HandleWakeup() {
        Schedule(TDuration::MilliSeconds(Cfg->StatisticsUpdateIntervalMs), new TEvents::TEvWakeup());

        TCpuTimer timer;
        PDisk->Mon.UpdatePercentileTrackers();
        PDisk->Mon.UpdateLights();
        const bool halt = PDisk->Mon.UpdateDeviceHaltCounters();
        PDisk->Mon.UpdateStats();
        ui64 updatePercentileTrackersCycles = timer.Elapsed();

        if (halt) {
            Send(SelfId(), new TEvDeviceError("device halt too long"));
        }

        TEvWhiteboardReportResult *response = new TEvWhiteboardReportResult();
        response->PDiskState = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate>();
        response->VDiskStateVect.reserve(16); // Pessimistic upper estimate of a number of owners
        THolder<TWhiteboardReport> request(PDisk->ReqCreator.CreateFromArgs<TWhiteboardReport>(SelfId(), response));
        ui64 whiteboardReportCycles = 0;
        ui64 updateSchedulerCycles = 0;
        if (!IsFormattingNow && AtomicGet(PDisk->IsStarted)) {
            PDisk->InputRequest(request.Release());

            // Update the current scheduler
            whiteboardReportCycles = timer.Elapsed();
            updateSchedulerCycles = 0;
        } else {
            PDisk->WhiteboardReport(*request); // Send TEvWhiteboardReportResult inside
        }


        LWPROBE(PDiskHandleWakeup, PCtx->PDiskId,
                HPMilliSecondsFloat(updatePercentileTrackersCycles),
                HPMilliSecondsFloat(whiteboardReportCycles),
                HPMilliSecondsFloat(updateSchedulerCycles));
    }

    void Handle(NPDisk::TEvWhiteboardReportResult::TPtr &ev) {
        NPDisk::TEvWhiteboardReportResult *result = ev->Get();
        Send(NodeWhiteboardServiceId, result->PDiskState.Release());
        P_LOG(PRI_TRACE, BSP01, "handle TEvWhiteboardReportResult", (Event, result->ToString()));
        for (auto& p : result->VDiskStateVect) {
            Send(std::get<0>(p),
                    new NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate(std::move(std::get<1>(p))));
        }
        if (result->DiskMetrics) {
            Send(NodeWardenServiceId, result->DiskMetrics.Release());
        }
        bool sendFlags = false;
        RealtimeFlag.Update(sendFlags);
        DeviceFlag.Update(sendFlags);
        AtomicSet(PDisk->NonRealTimeMs, RealtimeFlag.GetRedMsPs());
        AtomicSet(PDisk->SlowDeviceMs, DeviceFlag.GetRedMsPs());
        if (sendFlags) {
            Send(NodeWhiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate(
                PCtx->PDiskId, RealtimeFlag.Get(), DeviceFlag.Get()));
        }
    }

    void Handle(NPDisk::TEvDeviceError::TPtr &ev) {
        P_LOG(PRI_ERROR, BSP01, "Actor recieved device error", (Details, ev->Get()->Info));
        *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::DeviceIoError;
        *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
        *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDeviceIoError;
        PDisk->ErrorStr = ev->Get()->Info;
        InitError("io error");
    }

    void Handle(TEvBlobStorage::TEvAskWardenRestartPDiskResult::TPtr &ev) {
        bool restartAllowed = ev->Get()->RestartAllowed;

        EInitPhase initPhase = PDisk->InitPhase.load();

        bool isReadingLog = initPhase == EInitPhase::ReadingSysLog || initPhase == EInitPhase::ReadingLog;

        if ((isReadingLog && CurrentStateFunc() != &TPDiskActor::StateError) || IsFormattingNow) {
            // If disk is in the process of initialization (reading log) and it is not in error state, or disk is being formatted,
            // then it can not restart right now because it might cause a race condition.
            P_LOG(PRI_NOTICE, BSP01, "Received TEvAskWardenRestartPDiskResult while PDisk is still initializing, discard restart");

            if (PendingRestartResponse) {
                TString s("Unable to restart PDisk, it is initializing");
                PendingRestartResponse(false, s);
                PendingRestartResponse = {};
            }

            Send(ev->Sender, new TEvBlobStorage::TEvNotifyWardenPDiskRestarted(PCtx->PDiskId, NKikimrProto::EReplyStatus::NOTREADY));

            return;
        }

        if (PendingRestartResponse) {
            PendingRestartResponse(restartAllowed, ev->Get()->Details);
            PendingRestartResponse = {};
        }

        if (restartAllowed) {
            NPDisk::TMainKey newMainKey = ev->Get()->MainKey;

            SecureWipeBuffer((ui8*)ev->Get()->MainKey.Keys.data(), sizeof(NPDisk::TKey) * ev->Get()->MainKey.Keys.size());

            P_LOG(PRI_NOTICE, BSP01, "Going to restart PDisk since received TEvAskWardenRestartPDiskResult");

            const TActorIdentity& thisActorId = SelfId();
            ui32 nodeId = thisActorId.NodeId();
            ui32 poolId = thisActorId.PoolID();
            ui32 pdiskId = PCtx->PDiskId;

            PDisk->Stop();

            TIntrusivePtr<TPDiskConfig> actorCfg = std::move(Cfg);

            auto& newCfg = ev->Get()->Config;

            if (newCfg) {
                Y_VERIFY_S(newCfg->PDiskId == pdiskId,
                        "New config's PDiskId# " << newCfg->PDiskId << " is not equal to real PDiskId# " << pdiskId);

                actorCfg = std::move(newCfg);
            }

            const TActorContext& actorCtx = ActorContext();

            auto& counters = AppData(actorCtx)->Counters;

            TExecutorThread& executorThread = actorCtx.ExecutorThread;

            PassAway();

            CreatePDiskActor(executorThread, counters, actorCfg, newMainKey, pdiskId, poolId, nodeId);

            Send(ev->Sender, new TEvBlobStorage::TEvNotifyWardenPDiskRestarted(pdiskId));
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        const TCgiParameters &cgi = ev->Get()->Request.GetPostParams();

        bool enableChunkLocking = AppData()->FeatureFlags.GetEnableChunkLocking();

        if (enableChunkLocking) {
            using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
            if (cgi.Has("chunkLockByCount")) {
                TEvChunkLock::ELockFrom lockFrom = TEvChunkLock::LockFromByName(cgi.Get("lockFrom"));
                ui32 count = strtoul(cgi.Get("count").c_str(), nullptr, 10);
                if (lockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA) {
                    if (cgi.Has("byVDiskId")) {
                        bool isGenSet = true;
                        auto vdiskId = VDiskIDFromString(cgi.Get("vdiskId"), &isGenSet);
                        TEvChunkLock evLock(lockFrom, vdiskId, isGenSet, count, TColor::GREEN);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                        PDisk->InputRequest(request);
                    } else {
                        ui8 owner = strtoul(cgi.Get("owner").c_str(), nullptr, 10);
                        TEvChunkLock evLock(lockFrom, owner, count, TColor::GREEN);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                        PDisk->InputRequest(request);
                    }
                } else {
                    TEvChunkLock evLock(lockFrom, count, TColor::GREEN);
                    auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                    PDisk->InputRequest(request);
                }
            } else if (cgi.Has("chunkLockByColor")) {
                TEvChunkLock::ELockFrom lockFrom = TEvChunkLock::LockFromByName(cgi.Get("lockFrom"));
                TColor::E color = ColorByName(cgi.Get("spaceColor"));
                if (lockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA) {
                    if (cgi.Has("byVDiskId")) {
                        bool isGenSet = true;
                        auto vdiskId = VDiskIDFromString(cgi.Get("vdiskId"), &isGenSet);
                        TEvChunkLock evLock(lockFrom, vdiskId, isGenSet, 0, color);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                        PDisk->InputRequest(request);
                    } else {
                        ui8 owner = strtoul(cgi.Get("owner").c_str(), nullptr, 10);
                        TEvChunkLock evLock(lockFrom, owner, 0, color);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                        PDisk->InputRequest(request);
                    }
                } else {
                    TEvChunkLock evLock(lockFrom, 0, color);
                    auto* request = PDisk->ReqCreator.CreateFromEv<TChunkLock>(evLock, ev->Sender);
                    PDisk->InputRequest(request);
                }
            } else if (cgi.Has("chunkUnlock")) {
                TEvChunkLock::ELockFrom lockFrom = TEvChunkLock::LockFromByName(cgi.Get("lockFrom"));
                if (lockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA) {
                    if (cgi.Has("byVDiskId")) {
                        bool isGenSet = true;
                        auto vdiskId = VDiskIDFromString(cgi.Get("vdiskId"), &isGenSet);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkUnlock>(
                            NPDisk::TEvChunkUnlock(lockFrom, vdiskId, isGenSet), ev->Sender);
                        PDisk->InputRequest(request);
                    } else {
                        ui8 owner = strtoul(cgi.Get("owner").c_str(), nullptr, 10);
                        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkUnlock>(
                            NPDisk::TEvChunkUnlock(lockFrom, owner), ev->Sender);
                        PDisk->InputRequest(request);
                    }
                } else {
                    auto* request = PDisk->ReqCreator.CreateFromEv<TChunkUnlock>(
                        NPDisk::TEvChunkUnlock(lockFrom), ev->Sender);
                    PDisk->InputRequest(request);
                }
            }
        }
        if (cgi.Has("restartPDisk")) {
            bool ignoreChecks = "true" == cgi.Get("ignoreChecks");

            ui32 cookieIdxPart = NextRestartRequestCookie++;
            ui64 fullCookie = (((ui64) PCtx->PDiskId) << 32) | cookieIdxPart; // This way cookie will be unique regardless of the disk.

            Send(NodeWardenServiceId, new TEvBlobStorage::TEvAskWardenRestartPDisk(PCtx->PDiskId, ignoreChecks), fullCookie);
            // Send responce later when restart command will be received.
            PendingRestartResponse = [this, actor = ev->Sender] (bool restartAllowed, TString& details) {
                TStringStream jsonBuilder;
                jsonBuilder << NMonitoring::HTTPOKJSON;

                jsonBuilder << "{\"result\":" << (restartAllowed ? "true" : "false");

                if (!restartAllowed) {
                    jsonBuilder << ", \"error\": \"" << details << "\"";
                }

                jsonBuilder << "}";

                auto result = std::make_unique<NMon::TEvHttpInfoRes>(jsonBuilder.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom);

                Send(actor, result.release());
            };
            return;
        } else if (cgi.Has("stopPDisk")) {
            if (Cfg->SectorMap) {
                *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::DeviceIoError;
                *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorFake;

                PDisk->Stop();
                InitError("Received Stop from web UI");
            }
            Send(ev->Sender, new NMon::TEvHttpInfoRes(""));
            return;
        }

        bool doGetSchedule = false;
        const auto& httpRequest = ev->Get()->Request;
        if (httpRequest.GetMethod() == HTTP_METHOD_GET) {
            /*
            TStringStream out;
            out << "HTTP/1.1 200 Ok\r\n"
                << "Content-Type: text/html\r\n"
                << "Access-Control-Allow-Origin: *\r\n"
                << "Connection: Close\r\n\r\n";
            ForsetiScheduler.OutputLog(out);
            TEvHttpInfoResult *reportResult = new TEvHttpInfoResult(httpInfo.EndCustomer);
            reportResult->HttpInfoRes = new NMon::TEvHttpInfoRes(out.Str(), 0,
                    NMon::IEvHttpInfoRes::EContentType::Custom);
            ActorSystem->Send(ev->Sender, reportResult);
            */

            if (httpRequest.GetParams().Get("mode") == "getschedule") {
                doGetSchedule = true;
            }
        }

        TStringStream deviceFlagStr;
        DeviceFlag.Render(deviceFlagStr);
        TStringStream realtimeFlagStr;
        RealtimeFlag.Render(realtimeFlagStr);
        TStringStream fairSchedulerStr;
        TStringStream outStr;
        outStr.Reserve(512 << 10);

        THolder<THttpInfo> req(PDisk->ReqCreator.CreateFromArgs<THttpInfo>(SelfId(), ev->Sender, outStr,
                    deviceFlagStr.Str(), realtimeFlagStr.Str(), fairSchedulerStr.Str(), PDisk->ErrorStr, doGetSchedule));
        if (AtomicGet(PDisk->IsStarted)) {
            PDisk->InputRequest(req.Release());
        } else {
            PDisk->HttpInfo(*req); // Sends TEvHttpInfoResult inside
        }
    }

    void Handle(NPDisk::TEvHttpInfoResult::TPtr &ev) {
        NPDisk::TEvHttpInfoResult *result = ev->Get();
        Send(result->EndCustomer, result->HttpInfoRes.Release());
    }

    void Handle(NPDisk::TEvReadLogContinue::TPtr &ev) {
        auto *request = PDisk->ReqCreator.CreateFromEvPtr<TLogReadContinue>(ev);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvLogSectorRestore::TPtr &ev) {
        auto *request = PDisk->ReqCreator.CreateFromEv<TLogSectorRestore>(*ev->Get(), SelfId());
        PDisk->InputRequest(request);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        auto sender = ev->Sender;
        TRequestBase *request = PDisk->ReqCreator.CreateFromEv<TUndelivered>(std::move(ev), sender);
        PDisk->InputRequest(request);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor state functions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    STRICT_STFUNC(StateInit,
            hFunc(NPDisk::TEvYardInit, InitHandle);
            hFunc(NPDisk::TEvCheckSpace, ErrorHandle);
            hFunc(NPDisk::TEvLog, ErrorHandle);
            hFunc(NPDisk::TEvLogResult, ErrorHandle);
            hFunc(NPDisk::TEvMultiLog, ErrorHandle);
            hFunc(NPDisk::TEvReadLog, ErrorHandle);
            hFunc(NPDisk::TEvChunkWrite, ErrorHandle);
            hFunc(NPDisk::TEvChunkRead, ErrorHandle);
            hFunc(NPDisk::TEvChunkWriteRaw, ErrorHandle);
            hFunc(NPDisk::TEvChunkReadRaw, ErrorHandle);
            hFunc(NPDisk::TEvHarakiri, ErrorHandle);
            hFunc(NPDisk::TEvSlay, InitHandle);
            hFunc(NPDisk::TEvChunkReserve, ErrorHandle);
            hFunc(NPDisk::TEvChunkForget, ErrorHandle);
            hFunc(NPDisk::TEvYardControl, InitHandle);
            hFunc(NPDisk::TEvAskForCutLog, ErrorHandle);
            hFunc(NPDisk::TEvWhiteboardReportResult, Handle);
            hFunc(NPDisk::TEvHttpInfoResult, Handle);
            hFunc(NPDisk::TEvReadLogContinue, Handle);
            hFunc(NPDisk::TEvLogSectorRestore, Handle);
            hFunc(NPDisk::TEvLogInitResult, InitHandle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NPDisk::TEvPDiskFormattingFinished, InitHandle);
            hFunc(NPDisk::TEvPDiskMetadataLoaded, InitHandle);
            hFunc(TEvReadFormatResult, InitHandle);
            hFunc(NPDisk::TEvReadLogResult, InitHandle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, InitHandle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);
            hFunc(NPDisk::TEvFormatReencryptionFinish, InitHandle);
            hFunc(NPDisk::TEvShredPDisk, InitHandle);
            hFunc(NPDisk::TEvPreShredCompactVDiskResult, InitHandle);
            hFunc(NPDisk::TEvShredVDiskResult, InitHandle);
            hFunc(NPDisk::TEvContinueShred, InitHandle);
            hFunc(NPDisk::TEvYardResize, InitHandle);
            hFunc(NPDisk::TEvChangeExpectedSlotCount, InitHandle);

            hFunc(TEvReadMetadata, Handle);
            hFunc(TEvWriteMetadata, Handle);
    )

    STRICT_STFUNC(StateOnline,
            hFunc(NPDisk::TEvYardInit, Handle);
            hFunc(NPDisk::TEvCheckSpace, Handle);
            hFunc(NPDisk::TEvLog, Handle);
            hFunc(NPDisk::TEvLogResult, Handle);
            hFunc(NPDisk::TEvMultiLog, Handle);
            hFunc(NPDisk::TEvReadLog, Handle);
            hFunc(NPDisk::TEvChunkWrite, Handle);
            hFunc(NPDisk::TEvChunkRead, Handle);
            hFunc(NPDisk::TEvChunkWriteRaw, Handle);
            hFunc(NPDisk::TEvChunkReadRaw, Handle);
            hFunc(NPDisk::TEvHarakiri, Handle);
            hFunc(NPDisk::TEvSlay, Handle);
            hFunc(NPDisk::TEvChunkReserve, Handle);
            hFunc(NPDisk::TEvChunkForget, Handle);
            hFunc(NPDisk::TEvChunkLock, Handle);
            hFunc(NPDisk::TEvChunkUnlock, Handle);
            hFunc(NPDisk::TEvYardControl, Handle);
            hFunc(NPDisk::TEvAskForCutLog, Handle);
            hFunc(NPDisk::TEvConfigureScheduler, Handle);
            hFunc(NPDisk::TEvWhiteboardReportResult, Handle);
            hFunc(NPDisk::TEvHttpInfoResult, Handle);
            hFunc(NPDisk::TEvReadLogContinue, Handle);
            hFunc(NPDisk::TEvLogSectorRestore, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NPDisk::TEvShredPDisk, Handle);
            hFunc(NPDisk::TEvPreShredCompactVDiskResult, Handle);
            hFunc(NPDisk::TEvShredVDiskResult, Handle);
            hFunc(NPDisk::TEvContinueShred, Handle);
            hFunc(NPDisk::TEvYardResize, Handle);
            hFunc(NPDisk::TEvChangeExpectedSlotCount, Handle);

            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);

            hFunc(TEvReadMetadata, Handle);
            hFunc(TEvWriteMetadata, Handle);
    )

    STRICT_STFUNC(StateError,
            hFunc(NPDisk::TEvYardInit, ErrorHandle);
            hFunc(NPDisk::TEvCheckSpace, ErrorHandle);
            hFunc(NPDisk::TEvLog, ErrorHandle);
            hFunc(NPDisk::TEvLogResult, ErrorHandle);
            hFunc(NPDisk::TEvMultiLog, ErrorHandle);
            hFunc(NPDisk::TEvReadLog, ErrorHandle);
            hFunc(NPDisk::TEvChunkWrite, ErrorHandle);
            hFunc(NPDisk::TEvChunkRead, ErrorHandle);
            hFunc(NPDisk::TEvChunkWriteRaw, ErrorHandle);
            hFunc(NPDisk::TEvChunkReadRaw, ErrorHandle);
            hFunc(NPDisk::TEvHarakiri, ErrorHandle);
            hFunc(NPDisk::TEvSlay, ErrorHandle);
            hFunc(NPDisk::TEvChunkReserve, ErrorHandle);
            hFunc(NPDisk::TEvChunkForget, ErrorHandle);
            hFunc(NPDisk::TEvYardControl, ErrorHandle);
            hFunc(NPDisk::TEvAskForCutLog, ErrorHandle);
            hFunc(NPDisk::TEvReadFormatResult, ErrorHandle);
            hFunc(NPDisk::TEvWhiteboardReportResult, Handle);
            hFunc(NPDisk::TEvHttpInfoResult, Handle);
            hFunc(NPDisk::TEvReadLogContinue, Handle);
            hFunc(NPDisk::TEvLogSectorRestore, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NPDisk::TEvShredPDisk, ErrorHandle);
            hFunc(NPDisk::TEvPreShredCompactVDiskResult, ErrorHandle);
            hFunc(NPDisk::TEvShredVDiskResult, ErrorHandle);
            hFunc(NPDisk::TEvContinueShred, ErrorHandle);
            hFunc(NPDisk::TEvYardResize, ErrorHandle);
            hFunc(NPDisk::TEvChangeExpectedSlotCount, ErrorHandle);

            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);

            hFunc(TEvReadMetadata, Handle);
            hFunc(TEvWriteMetadata, Handle);
    )
};

} // NPDisk

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Creation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
IActor* CreatePDisk(const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new NPDisk::TPDiskActor(cfg, mainKey, counters);
}

void TRealPDiskServiceFactory::Create(const TActorContext &ctx, ui32 pDiskID,
        const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId) {
    CreatePDiskActor(ctx.ExecutorThread, AppData(ctx)->Counters, cfg, mainKey, pDiskID, poolId, nodeId);
}

} // NKikimr
