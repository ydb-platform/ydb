#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_factory.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_thread.h"
#include "blobstorage_pdisk_tools.h"
#include "blobstorage_pdisk_util_countedqueueoneone.h"
#include "blobstorage_pdisk_util_cputimer.h"
#include "blobstorage_pdisk_writer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/schlab/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/random/entropy.h>
#include <util/string/split.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

void CreatePDiskActor(
        TGenericExecutorThread& executorThread,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TIntrusivePtr<TPDiskConfig> &cfg,
        const NPDisk::TMainKey &mainKey,
        ui32 pDiskID, ui32 poolId, ui32 nodeId
) {

    TActorId actorId = executorThread.RegisterActor(CreatePDisk(cfg, mainKey, counters), TMailboxType::ReadAsFilled, poolId);

    TActorId pDiskServiceId = MakeBlobStoragePDiskID(nodeId, pDiskID);

    executorThread.ActorSystem->RegisterLocalService(pDiskServiceId, actorId);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Actor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDiskActor : public TActorBootstrapped<TPDiskActor> {
    struct TInitQueueItem {
        TOwnerRound OwnerRound;
        TVDiskID VDisk;
        ui64 PDiskGuid;
        TActorId Sender;
        TActorId CutLogId;
        TActorId WhiteboardProxyId;
        ui32 SlotId;

        TInitQueueItem(TOwnerRound ownerRound, TVDiskID vDisk, ui64 pDiskGuid, TActorId sender, TActorId cutLogId,
                TActorId whiteboardProxyId, ui32 slotId)
            : OwnerRound(ownerRound)
            , VDisk(vDisk)
            , PDiskGuid(pDiskGuid)
            , Sender(sender)
            , CutLogId(cutLogId)
            , WhiteboardProxyId(whiteboardProxyId)
            , SlotId(slotId)
        {}
    };

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
        Y_ABORT_UNLESS(MainKey.IsInitialized);
    }

    ~TPDiskActor() {
        SecureWipeBuffer((ui8*)MainKey.Keys.data(), sizeof(NPDisk::TKey) * MainKey.Keys.size());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor handlers
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bootstrap state
    void Bootstrap(const TActorContext &ctx) {
        auto mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            NMonitoring::TIndexMonPage *pdisksMonPage = actorsMonPage->RegisterIndexPage("pdisks", "PDisks");

            TString path = Sprintf("pdisk%09" PRIu32, (ui32)Cfg->PDiskId);
            TString name = Sprintf("PDisk%09" PRIu32, (ui32)Cfg->PDiskId);
            mon->RegisterActorPage(pdisksMonPage, path, name, false, ctx.ExecutorThread.ActorSystem,
                SelfId());
        }
        NodeWhiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        NodeWardenServiceId = MakeBlobStorageNodeWardenID(SelfId().NodeId());

        Schedule(TDuration::MilliSeconds(Cfg->StatisticsUpdateIntervalMs), new TEvents::TEvWakeup());

        StartPDiskThread();
    }

    void StartPDiskThread() {
        PDisk = new TPDisk(Cfg, PDiskCounters);

        RealtimeFlag.RemoveSources();
        DeviceFlag.RemoveSources();
        DeviceFlag.AddSource(PDisk->Mon.L6);

        bool isOk = PDisk->Initialize(TlsActivationContext->ActorSystem(), SelfId());

        if (!MainKey) {
            TStringStream str;
            str << "PDiskId# " << (ui32)PDisk->PDiskId
                << " MainKey is invalid, ErrorReason# " << MainKey.ErrorReason;
            InitError(str.Str());
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
        } else if (!isOk) {
            TStringStream str;
            str << "PDiskId# " << (ui32)PDisk->PDiskId
                << " bootstrapped to the StateError, reason# " << PDisk->ErrorStr
                << " Can not be initialized";
            InitError(str.Str());
            str << " Config: " << Cfg->ToString();
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
        } else {
            PDisk->InitiateReadSysLog(SelfId());
            StateErrorReason =
                "PDisk is in StateInit, wait for PDisk to read sys log. Did you ckeck EvYardInit result? Marker# BSY09";
            Become(&TThis::StateInit);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Init state
    void InitError(const TString &errorReason) {
        Become(&TThis::StateError);
        for (TList<TInitQueueItem>::iterator it = InitQueue.begin(); it != InitQueue.end(); ++it) {
            Send(it->Sender, new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, errorReason));
            if (PDisk) {
                PDisk->Mon.YardInit.CountResponse();
            }
        }
        InitQueue.clear();
        TStringStream str;
        str << "PDisk is in StateError, reason# " << errorReason;
        StateErrorReason = str.Str();
        if (PDisk) {
            PDisk->ErrorStr = StateErrorReason;
            auto* request = PDisk->ReqCreator.CreateFromArgs<TStopDevice>();
            PDisk->InputRequest(request);
        }

        if (ControledStartResult) {
            auto *ev = ControledStartResult->Get<TEvYardControlResult>();
            ev->Status = NKikimrProto::CORRUPTED;
            ev->ErrorReason = StateErrorReason;
            TlsActivationContext->Send(ControledStartResult.Release());
        }
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
            StartPDiskThread();
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::BS_PDISK,
                    "PDiskId# " << PDisk->PDiskId << " device formatting done");
        } else {
            PDisk.Reset(new TPDisk(Cfg, PDiskCounters));
            PDisk->Initialize(TlsActivationContext->ActorSystem(), SelfId());
            Y_ABORT_UNLESS(PDisk->PDiskThread.Running());

            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDiskCannotBeFormated;

            PDisk->ErrorStr = ToString("Can not be formated! Reason# ") + ev->Get()->ErrorStr;

            TStringStream str;
            str << "PDiskId# " << (ui32)PDisk->PDiskId
                << " Can not be formated! Reason# " << ev->Get()->ErrorStr
                << " Switching to StateError. Config: " << Cfg->ToString();
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
            InitError(str.Str());
        }
    }

    void CheckMagicSector(ui8 *magicData, ui32 magicDataSize) {
        bool isFormatMagicValid = PDisk->IsFormatMagicValid(magicData, magicDataSize);
        if (isFormatMagicValid) {
            IsMagicAlreadyChecked = true;
            IsFormattingNow = true;
            // Stop PDiskThread but use PDisk object for creation of http pages
            PDisk->Stop();
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingDeviceFormattingAndTrimming;
            PDisk->ErrorStr = "Magic sector is present on disk, now going to format device";
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId << PDisk->ErrorStr);

            // Is used to pass parameters into formatting thread, because TThread can pass only void*
            using TCookieType = std::tuple<TPDiskActor*, TActorSystem*, TActorId>;
            FormattingThread.Reset(new TThread(
                    [] (void *cookie) -> void* {
                        auto params = static_cast<TCookieType*>(cookie);
                        TPDiskActor *actor = std::get<0>(*params);
                        TActorSystem *actorSystem = std::get<1>(*params);
                        TActorId pDiskActor = std::get<2>(*params);
                        delete params;

                        NPDisk::TKey chunkKey;
                        NPDisk::TKey logKey;
                        NPDisk::TKey sysLogKey;
                        EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
                        EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
                        EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));
                        TPDiskConfig *cfg = actor->Cfg.Get();

                        try {
                            try {
                                FormatPDisk(cfg->GetDevicePath(), 0, cfg->SectorSize, cfg->ChunkSize,
                                    cfg->PDiskGuid, chunkKey, logKey, sysLogKey, actor->MainKey.Keys.back(), TString(), false,
                                    cfg->FeatureFlags.GetTrimEntireDeviceOnStartup(), cfg->SectorMap,
                                    cfg->FeatureFlags.GetEnableSmallDiskOptimization());
                            } catch (NPDisk::TPDiskFormatBigChunkException) {
                                FormatPDisk(cfg->GetDevicePath(), 0, cfg->SectorSize, NPDisk::SmallDiskMaximumChunkSize,
                                    cfg->PDiskGuid, chunkKey, logKey, sysLogKey, actor->MainKey.Keys.back(), TString(), false,
                                    cfg->FeatureFlags.GetTrimEntireDeviceOnStartup(), cfg->SectorMap,
                                    cfg->FeatureFlags.GetEnableSmallDiskOptimization());
                            }
                            actorSystem->Send(pDiskActor, new TEvPDiskFormattingFinished(true, ""));
                        } catch (yexception ex) {
                            LOG_ERROR_S(*actorSystem, NKikimrServices::BS_PDISK, "Formatting error, what#" << ex.what());
                            actorSystem->Send(pDiskActor, new TEvPDiskFormattingFinished(false, ex.what()));
                        }
                        return nullptr;
                    },
                    new TCookieType(this, TlsActivationContext->ActorSystem(), SelfId())));

            FormattingThread->Start();
        } else {
            SecureWipeBuffer((ui8*)MainKey.Keys.data(), sizeof(NPDisk::TKey) * MainKey.Keys.size());
            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorPDiskCannotBeInitialised;
            if (!IsMagicAlreadyChecked) {
                PDisk->ErrorStr = "Format is incomplete. Magic sector is not present on disk. Maybe wrong PDiskKey";
            } else {
                PDisk->ErrorStr = "Format is incomplete. Magic sector is present and new format was written";
            }
            TStringStream str;
            str << "PDiskId# " << PDisk->PDiskId
                << " Can not be initialized! " << PDisk->ErrorStr;
            for (ui32 i = 0; i < Cfg->HashedMainKey.size(); ++i) {
                str << " Hash(NewMainKey[" << i << "])# " << Cfg->HashedMainKey[i];
            }
            InitError(str.Str());
            str << " Config: " << Cfg->ToString();
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
        }
    }

    void ReencryptDiskFormat(const TDiskFormat& format, const NPDisk::TKey& newMainKey) {
        IsFormattingNow = true;
        // Stop PDiskThread but use PDisk object for creation of http pages
        PDisk->Stop();
        *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingReencryptingFormat;
        PDisk->ErrorStr = " Format sectors are encrypted with an old PDisk key, reencryption started";
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId << PDisk->ErrorStr);

        // Is used to pass parameters into formatting thread, because TThread can pass only void*
        using TCookieType = std::tuple<TDiskFormat, NPDisk::TKey, TIntrusivePtr<TPDiskConfig>, NActors::TActorSystem*, TActorId>;
        FormattingThread.Reset(new TThread(
            [] (void *cookie) -> void* {
                std::unique_ptr<TCookieType> params(static_cast<TCookieType*>(cookie));
                TDiskFormat format = std::get<0>(*params);
                NPDisk::TKey mainKey = std::get<1>(*params);
                TIntrusivePtr<TPDiskConfig> cfg = std::get<2>(*params);
                const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);
                NActors::TActorSystem* actorSystem = std::get<3>(*params);
                TActorId pdiskActor = std::get<4>(*params);

                THolder<NPDisk::TPDisk> pDisk(new NPDisk::TPDisk(cfg, counters));

                pDisk->Initialize(actorSystem, TActorId());

                if (!pDisk->BlockDevice->IsGood()) {
                    ythrow yexception() << "Failed to initialize temporal PDisk for format rewriting, info# " << pDisk->BlockDevice->DebugInfo();
                }

                try {
                    pDisk->WriteApplyFormatRecord(format, mainKey);
                    actorSystem->Send(pdiskActor, new TEvFormatReencryptionFinish(true, ""));
                } catch (yexception ex) {
                    LOG_ERROR_S(*actorSystem, NKikimrServices::BS_PDISK, "Reencryption error, what#" << ex.what());
                    actorSystem->Send(pdiskActor, new TEvFormatReencryptionFinish(false, ex.what()));
                }
                return nullptr;
            },
            new TCookieType(format, newMainKey, PDisk->Cfg, TlsActivationContext->ActorSystem(), SelfId())
        ));
        FormattingThread->Start();
    }

    void InitHandle(TEvFormatReencryptionFinish::TPtr &ev) {
        FormattingThread->Join();
        IsFormattingNow = false;
        if (ev->Get()->Success) {
            StartPDiskThread();
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::BS_PDISK,
                    "PDiskId# " << PDisk->PDiskId << " format chunks reencryption finished");
        } else {
            PDisk.Reset(new TPDisk(Cfg, PDiskCounters));
            PDisk->Initialize(TlsActivationContext->ActorSystem(), SelfId());
            Y_ABORT_UNLESS(PDisk->PDiskThread.Running());

            *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
            *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
            *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDiskCannotBeFormated;

            PDisk->ErrorStr = ToString("Format chunks cannot be reencrypted! Reason# ") + ev->Get()->ErrorReason;

            TStringStream str;
            str << "PDiskId# " << (ui32)PDisk->PDiskId
                << " Format chunks cannot be reencrypted! Reason# " << ev->Get()->ErrorReason
                << " Switching to StateError. Config: " << Cfg->ToString();
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
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
            if (res.IsReencryptionRequired) {
                // Format reencryption required
                ReencryptDiskFormat(PDisk->Format, MainKey.Keys.back());
                // We still need main key after restart
                // SecureWipeBuffer((ui8*)MainKey.data(), sizeof(NPDisk::TKey) * MainKey.size());
            } else {
                // Format is read OK
                SecureWipeBuffer((ui8*)MainKey.Keys.data(), sizeof(NPDisk::TKey) * MainKey.Keys.size());
                LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                        << " Successfully read format record# " << PDisk->Format.ToString());
                TString info;
                if (!PDisk->CheckGuid(&info)) {
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
                    *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialFormatReadDueToGuid;
                    PDisk->ErrorStr = TStringBuilder() << "Can't start due to a guid error " << info;
                    TStringStream str;
                    str << "PDiskId# " << PDisk->PDiskId << PDisk->ErrorStr;
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
                    InitError(str.Str());
                } else if (!PDisk->CheckFormatComplete()) {
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatReadError;
                    *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialFormatReadIncompleteFormat;
                    PDisk->ErrorStr = "Can't start due to incomplete format!";
                    TStringStream str;
                    str << "PDiskId# " << PDisk->PDiskId << " " << PDisk->ErrorStr << " "
                        << "Please, do not turn off your server or remove your storage device while formatting. "
                        << "We are sure you did this or something even more creative, like killing the formatter.";
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
                    InitError(str.Str());
                } else {
                    // PDisk GUID is OK and format is complete
                    *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialSysLogRead;
                    *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingSysLogRead;
                    PDisk->Format.InitMagic();
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
            str << "PDiskId# " << PDisk->PDiskId <<
                " Can't start due to a log processing error! ErrorStr# \"" << evLogInitResult.ErrorStr << "\"";
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PDISK, str.Str());
            InitError(str.Str());
        }
    }

    void InitSuccess() {
        Become(&TThis::StateOnline);
        for (TList<TInitQueueItem>::iterator it = InitQueue.begin(); it != InitQueue.end(); ++it) {
            NPDisk::TEvYardInit evInit(it->OwnerRound, it->VDisk, it->PDiskGuid, it->CutLogId, it->WhiteboardProxyId,
                it->SlotId);
            auto* request = PDisk->ReqCreator.CreateFromEv<TYardInit>(evInit, it->Sender);
            PDisk->InputRequest(request);
        }
        InitQueue.clear();
        if (ControledStartResult) {
            TlsActivationContext->Send(ControledStartResult.Release());
        }
    }

    void InitHandle(NPDisk::TEvYardInit::TPtr &ev) {
        const NPDisk::TEvYardInit &evYardInit = *ev->Get();
        InitQueue.emplace_back(evYardInit.OwnerRound, evYardInit.VDisk, evYardInit.PDiskGuid,
            ev->Sender, evYardInit.CutLogID, evYardInit.WhiteboardProxyId, evYardInit.SlotId);
    }

    void InitHandle(NPDisk::TEvYardControl::TPtr &ev) {

        const NPDisk::TEvYardControl &evControl = *ev->Get();
        switch (evControl.Action) {
        case TEvYardControl::PDiskStart:
            ControledStartResult = MakeHolder<IEventHandle>(ev->Sender, SelfId(),
                    new TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
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
        str << "PDiskId# " << (ui32)PDisk->PDiskId << " is still initializing, please wait";
        Send(ev->Sender, new NPDisk::TEvSlayResult(NKikimrProto::NOTREADY, 0,
                    evSlay.VDiskId, evSlay.SlayOwnerRound, evSlay.PDiskId, evSlay.VSlotId, str.Str()));
        PDisk->Mon.YardSlay.CountResponse();
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
        Send(ev->Sender, new NPDisk::TEvCheckSpaceResult(NKikimrProto::CORRUPTED, 0, 0, 0, 0, 0, StateErrorReason));
        PDisk->Mon.CheckSpace.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvLog::TPtr &ev) {
        const NPDisk::TEvLog &evLog = *ev->Get();
        TStringStream str;
        str << "PDiskId# " << PDisk->PDiskId;
        str << " TEvLog error because PDisk State# ";
        if (CurrentStateFunc() == &TPDiskActor::StateInit) {
            str << "Init, wait for PDisk to initialize. Did you ckeck EvYardInit result? Marker# BSY08";
        } else if (CurrentStateFunc() == &TPDiskActor::StateError) {
            str << "Error, there is a terminal internal error in PDisk. Did you check EvYardInit result? Marker# BSY07";
        } else {
            str << "Unknown, something went very wrong in PDisk. Marker# BSY06";
        }
        str << " StateErrorReason# " << StateErrorReason;
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, str.Str()));
        result->Results.push_back(NPDisk::TEvLogResult::TRecord(evLog.Lsn, evLog.Cookie));
        PDisk->Mon.WriteLog.CountRequest(0);
        Send(ev->Sender, result.Release());
        PDisk->Mon.WriteLog.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvMultiLog::TPtr &ev) {
        const NPDisk::TEvMultiLog &evMultiLog = *ev->Get();
        TStringStream str;
        str << "PDiskId# " << PDisk->PDiskId;
        str << " TEvBatchedLogs error because PDisk State# ";
        if (CurrentStateFunc() == &TPDiskActor::StateInit) {
            str << "Init, wait for PDisk to initialize. Did you ckeck EvYardInit result? Marker# BSY10";
        } else if (CurrentStateFunc() == &TPDiskActor::StateError) {
            str << "Error, there is a terminal internal error in PDisk. Did you check EvYardInit result? Marker# BSY11";
        } else {
            str << "Unknown, something went very wrong in PDisk. Marker# BSY12";
        }
        str << " StateErrorReason# " << StateErrorReason;
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, str.Str()));
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
        str << "PDiskId# " << PDisk->PDiskId;
        str << " TEvReadLog error because PDisk State# ";
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

        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s To: %" PRIu64 " Marker# BSY02",
            (ui32)PDisk->PDiskId, result->ToString().c_str(), (ui64)ev->Sender.LocalId());
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
        str << "PDiskId# " << (ui32)PDisk->PDiskId << " is in error state.";
        Send(ev->Sender, new NPDisk::TEvSlayResult(NKikimrProto::CORRUPTED, 0,
                    evSlay.VDiskId, evSlay.SlayOwnerRound, evSlay.PDiskId, evSlay.VSlotId, str.Str()));
        PDisk->Mon.YardSlay.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChunkReserve::TPtr &ev) {
        PDisk->Mon.ChunkReserve.CountRequest();
        Send(ev->Sender, new NPDisk::TEvChunkReserveResult(NKikimrProto::CORRUPTED, 0, StateErrorReason));
        PDisk->Mon.ChunkReserve.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvChunkForget::TPtr &ev) {
        PDisk->Mon.ChunkForget.CountRequest();
        Send(ev->Sender, new NPDisk::TEvChunkForgetResult(NKikimrProto::CORRUPTED, 0, StateErrorReason));
        PDisk->Mon.ChunkForget.CountResponse();
    }

    void ErrorHandle(NPDisk::TEvYardControl::TPtr &ev) {
        const NPDisk::TEvYardControl &evControl = *ev->Get();
        Y_ABORT_UNLESS(PDisk);

        PDisk->Mon.YardControl.CountRequest();

        switch (evControl.Action) {
        case TEvYardControl::PDiskStart:
        {
            auto *mainKey = static_cast<const NPDisk::TMainKey*>(evControl.Cookie);
            Y_ABORT_UNLESS(mainKey);
            MainKey = *mainKey;
            StartPDiskThread();
            ControledStartResult = MakeHolder<IEventHandle>(ev->Sender, SelfId(),
                    new TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
            break;
        }
        default:
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::CORRUPTED, evControl.Cookie, StateErrorReason));
            PDisk->Mon.YardControl.CountResponse();
            break;
        }
    }

    void ErrorHandle(NPDisk::TEvAskForCutLog::TPtr &ev) {
        // Just ignore the event, can't send cut log in this state.
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

    void CheckBurst(bool isSensitive, double burstMs) {
        Y_UNUSED(isSensitive);
        Y_UNUSED(burstMs);
    }

    void Handle(NPDisk::TEvLog::TPtr &ev) {
        double burstMs;
        TLogWrite* request = PDisk->ReqCreator.CreateLogWrite(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        CheckBurst(request->IsSensitive, burstMs);
        request->Orbit = std::move(ev->Get()->Orbit);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvMultiLog::TPtr &ev) {
        for (auto &[log, traceId] : ev->Get()->Logs) {
            double burstMs;
            TLogWrite* request = PDisk->ReqCreator.CreateLogWrite(*log, ev->Sender, burstMs, std::move(traceId));
            CheckBurst(request->IsSensitive, burstMs);
            request->Orbit = std::move(log->Orbit);
            PDisk->InputRequest(request);
        }
    }

    void Handle(NPDisk::TEvReadLog::TPtr &ev) {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s Marker# BSY01",
            (ui32)PDisk->PDiskId, ev->Get()->ToString().c_str());
        double burstMs;
        auto* request = PDisk->ReqCreator.CreateFromEvPtr<TLogRead>(ev, &burstMs);
        CheckBurst(request->IsSensitive, burstMs);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkWrite::TPtr &ev) {
        double burstMs;
        TChunkWrite* request = PDisk->ReqCreator.CreateChunkWrite(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        request->Orbit = std::move(ev->Get()->Orbit);
        CheckBurst(request->IsSensitive, burstMs);
        PDisk->InputRequest(request);
    }

    void Handle(NPDisk::TEvChunkRead::TPtr &ev) {
        double burstMs;
        TChunkRead* request = PDisk->ReqCreator.CreateChunkRead(*ev->Get(), ev->Sender, burstMs, std::move(ev->TraceId));
        request->DebugInfoGenerator = PDisk->DebugInfoGenerator;
        CheckBurst(request->IsSensitive, burstMs);
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

    void Handle(NPDisk::TEvChunkReserve::TPtr &ev) {
        auto* request = PDisk->ReqCreator.CreateFromEv<TChunkReserve>(*ev->Get(), ev->Sender);
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
            PDisk->Stop();
            InitError("Received TEvYardControl::PDiskStop");
            Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, evControl.Cookie, {}));
            break;
        case TEvYardControl::GetPDiskPointer:
            Y_ABORT_UNLESS(!evControl.Cookie);
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
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PDISK,
                "PDiskId# " << (ui32)PDisk->PDiskId << " " << ev->Get()->ToString());
        PDisk->Mon.YardConfigureScheduler.CountRequest();
        // Configure forseti scheduler weights
        auto* request = PDisk->ReqCreator.CreateFromEv<TConfigureScheduler>(*ev->Get(), ev->Sender);
        PDisk->InputRequest(request);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // All states

    void HandlePoison() {
        ui32 pdiskId = PDisk->PDiskId;
        PDisk.Reset();
        PassAway();
        LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << pdiskId
                << " HandlePoison, PDiskThread stopped");
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


        LWPROBE(PDiskHandleWakeup, PDisk->PDiskId,
                HPMilliSecondsFloat(updatePercentileTrackersCycles),
                HPMilliSecondsFloat(whiteboardReportCycles),
                HPMilliSecondsFloat(updateSchedulerCycles));
    }

    void Handle(NPDisk::TEvWhiteboardReportResult::TPtr &ev) {
        NPDisk::TEvWhiteboardReportResult *result = ev->Get();
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDisk->PDiskId
                << " handle TEvWhiteboardReportResult# " << result->ToString());
        Send(NodeWhiteboardServiceId, result->PDiskState.Release());
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
                PDisk->PDiskId, RealtimeFlag.Get(), DeviceFlag.Get()));
        }
    }

    void Handle(NPDisk::TEvDeviceError::TPtr &ev) {
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PDISK,
                "Actor recieved device error, info# " << ev->Get()->Info);
        *PDisk->Mon.PDiskState = NKikimrBlobStorage::TPDiskState::DeviceIoError;
        *PDisk->Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
        *PDisk->Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDeviceIoError;
        PDisk->ErrorStr = ev->Get()->Info;
        InitError("io error");
    }

    void Handle(TEvBlobStorage::TEvAskWardenRestartPDiskResult::TPtr &ev) {
        bool restartAllowed = ev->Get()->RestartAllowed;

        bool isReadingLog = PDisk->InitPhase == EInitPhase::ReadingSysLog || PDisk->InitPhase == EInitPhase::ReadingLog;

        if ((isReadingLog && CurrentStateFunc() != &TPDiskActor::StateError) || IsFormattingNow) {
            // If disk is in the process of initialization (reading log) and it is not in error state, or disk is being formatted,
            // then it can not restart right now because it might cause a race condition.
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " Received TEvAskWardenRestartPDiskResult while PDisk is still initializing, discard restart");

            if (PendingRestartResponse) {
                TString s("Unable to restart PDisk, it is initializing");
                PendingRestartResponse(false, s);
                PendingRestartResponse = {};
            }

            Send(ev->Sender, new TEvBlobStorage::TEvNotifyWardenPDiskRestarted(PDisk->PDiskId, NKikimrProto::EReplyStatus::NOTREADY));
            
            return;
        }

        if (PendingRestartResponse) {
            PendingRestartResponse(restartAllowed, ev->Get()->Details);
            PendingRestartResponse = {};
        }

        if (restartAllowed) {
            NPDisk::TMainKey newMainKey = ev->Get()->MainKey;

            SecureWipeBuffer((ui8*)ev->Get()->MainKey.Keys.data(), sizeof(NPDisk::TKey) * ev->Get()->MainKey.Keys.size());
            
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " Going to restart PDisk since recieved TEvAskWardenRestartPDiskResult");

            const TActorIdentity& thisActorId = SelfId();
            ui32 nodeId = thisActorId.NodeId();
            ui32 poolId = thisActorId.PoolID();
            ui32 pdiskId = PDisk->PDiskId;

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

            TGenericExecutorThread& executorThread = actorCtx.ExecutorThread;

            PassAway();
            
            CreatePDiskActor(executorThread, counters, actorCfg, newMainKey, pdiskId, poolId, nodeId);

            Send(ev->Sender, new TEvBlobStorage::TEvNotifyWardenPDiskRestarted(pdiskId));
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        const TCgiParameters &cgi = ev->Get()->Request.GetPostParams();

        bool enableChunkLocking = Cfg->FeatureFlags.GetEnableChunkLocking();

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
            ui32 cookieIdxPart = NextRestartRequestCookie++;
            ui64 fullCookie = (((ui64) PDisk->PDiskId) << 32) | cookieIdxPart; // This way cookie will be unique no matter the disk.

            Send(NodeWardenServiceId, new TEvBlobStorage::TEvAskWardenRestartPDisk(PDisk->PDiskId), fullCookie);
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
            hFunc(NPDisk::TEvMultiLog, ErrorHandle);
            hFunc(NPDisk::TEvReadLog, ErrorHandle);
            hFunc(NPDisk::TEvChunkWrite, ErrorHandle);
            hFunc(NPDisk::TEvChunkRead, ErrorHandle);
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
            hFunc(TEvReadFormatResult, InitHandle);
            hFunc(NPDisk::TEvReadLogResult, InitHandle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, InitHandle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);
            hFunc(NPDisk::TEvFormatReencryptionFinish, InitHandle);
    )

    STRICT_STFUNC(StateOnline,
            hFunc(NPDisk::TEvYardInit, Handle);
            hFunc(NPDisk::TEvCheckSpace, Handle);
            hFunc(NPDisk::TEvLog, Handle);
            hFunc(NPDisk::TEvMultiLog, Handle);
            hFunc(NPDisk::TEvReadLog, Handle);
            hFunc(NPDisk::TEvChunkWrite, Handle);
            hFunc(NPDisk::TEvChunkRead, Handle);
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

            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);
    )

    STRICT_STFUNC(StateError,
            hFunc(NPDisk::TEvYardInit, ErrorHandle);
            hFunc(NPDisk::TEvCheckSpace, ErrorHandle);
            hFunc(NPDisk::TEvLog, ErrorHandle);
            hFunc(NPDisk::TEvMultiLog, ErrorHandle);
            hFunc(NPDisk::TEvReadLog, ErrorHandle);
            hFunc(NPDisk::TEvChunkWrite, ErrorHandle);
            hFunc(NPDisk::TEvChunkRead, ErrorHandle);
            hFunc(NPDisk::TEvHarakiri, ErrorHandle);
            hFunc(NPDisk::TEvSlay, ErrorHandle);
            hFunc(NPDisk::TEvChunkReserve, ErrorHandle);
            hFunc(NPDisk::TEvChunkForget, ErrorHandle);
            hFunc(NPDisk::TEvYardControl, ErrorHandle);
            hFunc(NPDisk::TEvAskForCutLog, ErrorHandle);
            hFunc(NPDisk::TEvWhiteboardReportResult, Handle);
            hFunc(NPDisk::TEvHttpInfoResult, Handle);
            hFunc(NPDisk::TEvReadLogContinue, Handle);
            hFunc(NPDisk::TEvLogSectorRestore, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);

            cFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NPDisk::TEvDeviceError, Handle);
            hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);
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
