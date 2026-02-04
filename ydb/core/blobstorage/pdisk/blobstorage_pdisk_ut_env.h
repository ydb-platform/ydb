#pragma once

#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/util/random.h>

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_defs.h"
#include "blobstorage_pdisk_data.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

struct TActorTestContext {
    using EDiskMode = NPDisk::NSectorMap::EDiskMode;
public:
    struct TSettings {
        bool IsBad = false;
        bool UsePDiskMock = false;
        ui64 DiskSize = 0;
        EDiskMode DiskMode = EDiskMode::DM_NONE;
        ui32 ChunkSize = 128 * (1 << 20);
        bool SmallDisk = false;
        bool SuppressCompatibilityCheck = false;
        TString UsePath = {}; // If set, use this path instead of a one in a temp dir
        bool UseSectorMap = true; // If set, use sector map instead of a file
        TAutoPtr<TLogBackend> LogBackend = nullptr;
        bool ReadOnly = false;
        bool InitiallyZeroed = false; // Only for sector map. Zero first 1MiB on start.
        bool PlainDataChunks = false;
        std::optional<bool> EnableFormatAndMetadataEncryption;
        std::optional<bool> EnableSectorEncryption;
        std::optional<bool> RandomizeMagic = std::nullopt;
        std::optional<ui64> NonceRandNum = std::nullopt;
        bool UseRdmaAllocator = false;
    };

private:
    std::optional<TActorId> PDiskActor;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    THolder<TTestActorRuntime> Runtime;
    NPDisk::TPDisk *PDisk = nullptr;

public:
    TActorId Sender;
    NPDisk::TMainKey MainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence }, .IsInitialized = true };
    TTestContext TestCtx;
    TSettings Settings;
    // this pointer doesn't own the object (only Runtime does)
    NWilson::TFakeWilsonUploader *WilsonUploader = new NWilson::TFakeWilsonUploader;

    void DoFormatPDisk(ui64 guid, bool enableFormatAndMetadataEncryption = true, std::optional<bool> enableSectorEncryption = std::nullopt) {
        FormatPDiskForTest(TestCtx.Path, guid, Settings.ChunkSize, Settings.DiskSize,
            false, TestCtx.SectorMap, Settings.SmallDisk, Settings.PlainDataChunks, enableFormatAndMetadataEncryption,
            enableSectorEncryption, Settings.RandomizeMagic);
    }

    TIntrusivePtr<TPDiskConfig> DefaultPDiskConfig(bool isBad) {
        SafeEntropyPoolRead(&TestCtx.PDiskGuid, sizeof(TestCtx.PDiskGuid));
        ui64 formatGuid = TestCtx.PDiskGuid + static_cast<ui64>(isBad);

        if (Settings.InitiallyZeroed) {
            UNIT_ASSERT(Settings.UseSectorMap);

            if (Settings.DiskSize) {
                TestCtx.SectorMap->ForceSize(Settings.DiskSize);
            } else {
                ui64 diskSizeHeuristic = (ui64)Settings.ChunkSize * 1000;
                TestCtx.SectorMap->ForceSize(diskSizeHeuristic);
            }

            // Zero the first 1MB so that PDisk seems like it is a new disk.
            TestCtx.SectorMap->ZeroInit(1_MB / NPDisk::NSectorMap::SECTOR_SIZE);
        }

        // not set by user, keep old behaviour
        if (!Settings.EnableSectorEncryption.has_value()) {
            Settings.EnableSectorEncryption = !TestCtx.SectorMap;
        }

        // here old behaviour is to always encrypt format
        if (!Settings.EnableFormatAndMetadataEncryption.has_value()) {
            Settings.EnableFormatAndMetadataEncryption = true;
        }

        if (!Settings.ReadOnly && !Settings.InitiallyZeroed) {
            DoFormatPDisk(formatGuid, *Settings.EnableFormatAndMetadataEncryption, *Settings.EnableSectorEncryption);
        }

        ui64 pDiskCategory = 0;
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(TestCtx.Path, TestCtx.PDiskGuid, 1, pDiskCategory);
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->ChunkSize = Settings.ChunkSize;
        pDiskConfig->SectorMap = TestCtx.SectorMap;
        pDiskConfig->EnableFormatAndMetadataEncryption = *Settings.EnableFormatAndMetadataEncryption;
        pDiskConfig->FeatureFlags.SetEnablePDiskDataEncryption(*Settings.EnableSectorEncryption);
        pDiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(Settings.SmallDisk);
        pDiskConfig->FeatureFlags.SetSuppressCompatibilityCheck(Settings.SuppressCompatibilityCheck);
        pDiskConfig->FeatureFlags.SetEnablePDiskLogForSmallDisks(false);
        pDiskConfig->ReadOnly = Settings.ReadOnly;
        pDiskConfig->PlainDataChunks = Settings.PlainDataChunks;
        pDiskConfig->NonceRandNum = Settings.NonceRandNum;

        return pDiskConfig;
    }

    TActorTestContext(TSettings settings)
        : Runtime(new TTestActorRuntime(1, 1, true, settings.UseRdmaAllocator))
        , TestCtx(settings.UseSectorMap, settings.DiskMode, settings.DiskSize, settings.UsePath)
        , Settings(settings)
    {
        auto appData = MakeHolder<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        appData->IoContextFactory = IoContext.get();

        if (Settings.LogBackend) {
            Runtime->SetLogBackend(Settings.LogBackend);
        } else {
            Runtime->SetLogBackend(IsLowVerbose ? CreateStderrBackend() : CreateNullBackend());
        }
        Runtime->Initialize(TTestActorRuntime::TEgg{appData.Release(), nullptr, {}, {}, {}});
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_NOTICE);
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK_SYSLOG, NLog::PRI_NOTICE);
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK_TEST, NLog::PRI_DEBUG);
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK_SHRED, NLog::PRI_DEBUG);
        Sender = Runtime->AllocateEdgeActor();

        TActorId uploaderId = Runtime->Register(WilsonUploader);
        Runtime->RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId);

        auto cfg = DefaultPDiskConfig(Settings.IsBad);
        UpdateConfigRecreatePDisk(cfg);
    }

    TIntrusivePtr<TPDiskConfig> GetPDiskConfig() {
        if (!Settings.UsePDiskMock) {
            return GetPDisk()->Cfg;
        }
        return nullptr;
    }

    TTestActorRuntime* GetRuntime() {
        return Runtime.Get();
    }

    void UpdateConfigRecreatePDisk(TIntrusivePtr<TPDiskConfig> cfg, bool reformat = false) {
        if (PDiskActor) {
            TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                    NKikimrProto::OK);
            PDisk = nullptr;
            Runtime->Send(new IEventHandle(*PDiskActor, Sender, new TKikimrEvents::TEvPoisonPill));
        }

        if (reformat) {
            DoFormatPDisk(TestCtx.PDiskGuid + static_cast<ui64>(Settings.IsBad),
                cfg->EnableFormatAndMetadataEncryption, cfg->FeatureFlags.GetEnablePDiskDataEncryption()
            );
        }

        if (Settings.UsePDiskMock) {
            ui32 nodeId = 1;
            ui64 size = ui64(10) << 40;
            TPDiskMockState::TPtr state(new TPDiskMockState((ui32)nodeId, (ui32)cfg->PDiskId, (ui64)cfg->PDiskGuid, (ui64)size, (ui32)cfg->ChunkSize));
            PDiskActor = Runtime->Register(CreatePDiskMockActor(state));
        } else {
            auto mainCounters = TIntrusivePtr<::NMonitoring::TDynamicCounters>(new ::NMonitoring::TDynamicCounters());
            IActor* pDiskActor = CreatePDisk(cfg.Get(), MainKey, mainCounters);
            PDiskActor = Runtime->Register(pDiskActor);
        }
    }

    void Send(IEventBase* ev) {
        auto evh = new IEventHandle(*PDiskActor, Sender, ev);
        // trace all events to check there is no VERIFY could happen
        evh->TraceId = NWilson::TTraceId::NewTraceId(NWilson::TTraceId::MAX_VERBOSITY, 4095);
        Runtime->Send(evh);
    }

    NPDisk::TPDisk *GetPDisk() {
        if (!PDisk && !Settings.UsePDiskMock) {
            // To be sure that pdisk actor is in StateOnline
            TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, (void*)(&MainKey)),
                    NKikimrProto::OK);

            const auto evControlRes = TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::GetPDiskPointer, nullptr),
                    NKikimrProto::OK);
            PDisk = reinterpret_cast<NPDisk::TPDisk*>(evControlRes->Cookie);

            PDiskActor = PDisk->PCtx->PDiskActor;
        }
        return PDisk;
    }

    void GracefulPDiskRestart(bool waitForRestart = true) {
        ui32 pdiskId = GetPDisk()->PCtx->PDiskId;

        Send(new TEvBlobStorage::TEvAskWardenRestartPDiskResult(pdiskId, MainKey, true, nullptr));

        if (waitForRestart) {
            const auto evInitRes = Recv<TEvBlobStorage::TEvNotifyWardenPDiskRestarted>();
            UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::EReplyStatus::OK, evInitRes->Status);
        }

        if (!Settings.UsePDiskMock) {
            TActorId wellKnownPDiskActorId = MakeBlobStoragePDiskID(PDiskActor->NodeId(), pdiskId);

            PDisk = nullptr;

            // We will temporarily use well know pdisk actor id, because restarted pdisk actor id is not yet known.
            PDiskActor = wellKnownPDiskActorId;
        }
    }

    template<typename T>
    auto SafeRunOnPDisk(T&& f) {
        TGuard<TMutex> g(GetPDisk()->StateMutex);
        return f(GetPDisk());
    }

    void RestartPDiskSync() {
        if (!Settings.UsePDiskMock) {
            TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                    NKikimrProto::OK);
            PDisk = nullptr;
            // wait initialization and update this->PDisk
            GetPDisk();
        }
    }

    template<typename TRes>
    THolder<TRes> Recv() {
        return Runtime->GrabEdgeEvent<TRes>();
    }

    template<typename TRes>
    THolder<TRes> TestResponse(IEventBase* ev, std::optional<NKikimrProto::EReplyStatus> status = std::nullopt) {
        if (ev) {
            Send(ev);
        }
        THolder<TRes> evRes = Recv<TRes>();

        if (status.has_value()) {
            UNIT_ASSERT_VALUES_EQUAL_C(evRes->Status, status.value(), evRes->ToString());
        }

        UNIT_ASSERT_C(evRes->Status == NKikimrProto::OK || !evRes->ErrorReason.empty(), "Status: " << NKikimrProto::EReplyStatus_Name(evRes->Status) << " ErrorReason: " << evRes->ErrorReason << " ToString: " << evRes->ToString());

        // Test that all ToString methods don't VERIFY
        Cnull << evRes->ToString();
        return evRes;
    }
};

struct TVDiskIDOwnerRound {
    TVDiskID VDiskID;
    ui64 OwnerRound;
};

void RecreateOwner(TActorTestContext& testCtx, TVDiskIDOwnerRound& vdisk);

enum class EChunkState {
    UNKNOWN,
    RESERVED,
    COMMIT_INFLIGHT,
    COMMITTED,
    DELETE_INFLIGHT,
    DELETED
};

struct TVDiskMock {
    static std::atomic<ui64> Idx;
    static std::atomic<ui64> OwnerRound;

    TActorTestContext *TestCtx;
    const TVDiskID VDiskID;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    ui64 LastUsedLsn = 0;
    ui64 FirstLsnToKeep = 1;

    TMap<EChunkState, TSet<TChunkIdx>> Chunks;

    TVDiskMock(TActorTestContext *testCtx, bool dynamicGroup = false)
        : TestCtx(testCtx)
        , VDiskID(MakeGroupId(dynamicGroup), 1, 0, 0, 0)
    {}

    static ui32 MakeGroupId(bool dynamicGroup) {
        const ui32 baseId = static_cast<ui32>(Idx.fetch_add(1));
        return dynamicGroup ? (baseId | 0x80000000u) : baseId;
    }

    TLsnSeg GetLsnSeg() {
        ++LastUsedLsn;
        return {LastUsedLsn, LastUsedLsn};
    };

    void InitFull(ui32 groupSizeInUnits = 0) {
        Init(groupSizeInUnits);
        ReadLog();
        SendEvLogImpl(1, {}, true);
    }

    void Init(ui32 groupSizeInUnits = 0) {
        const auto evInitRes = TestCtx->TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(OwnerRound.fetch_add(1), VDiskID,
                    TestCtx->TestCtx.PDiskGuid, TestCtx->Sender,
                    {}, Max<ui32>(), groupSizeInUnits),
                NKikimrProto::OK);

        PDiskParams = evInitRes->PDiskParams;

        TSet<TChunkIdx> commited = Chunks[EChunkState::COMMITTED];
        for (TChunkIdx chunk : evInitRes->OwnedChunks) {
            UNIT_ASSERT_C(commited.count(chunk), "misowned chunk# " << chunk);
            commited.erase(chunk);
        }
        UNIT_ASSERT_C(commited.empty(), "there are leaked chunks# " << FormatList(commited));
    }

    void ReserveChunk(ui32 chunkCountToReserve = 1) {
        const auto evReserveRes = TestCtx->TestResponse<NPDisk::TEvChunkReserveResult>(
                new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound, chunkCountToReserve),
                NKikimrProto::OK);
        UNIT_ASSERT(evReserveRes->ChunkIds.size() == chunkCountToReserve);
        for (const ui32 reservedChunk : evReserveRes->ChunkIds) {
            Chunks[EChunkState::RESERVED].emplace(reservedChunk);
        }
    }

    void CommitReservedChunks() {
        auto& reservedChunks = Chunks[EChunkState::RESERVED];
        NPDisk::TCommitRecord rec;
        rec.CommitChunks = TVector<TChunkIdx>(reservedChunks.begin(), reservedChunks.end());
        SendEvLogImpl(1, rec);
        Chunks[EChunkState::COMMITTED].insert(reservedChunks.begin(), reservedChunks.end());
        reservedChunks.clear();
    }

    void MarkCommitedChunksDirty() {
        auto& commited = Chunks[EChunkState::COMMITTED];
        TStackVec<TChunkIdx, 1> chunksToMark;
        NPDisk::TCommitRecord rec;
        for (auto it = commited.begin(); it != commited.end(); ++it) {
            rec.DirtyChunks.push_back(*it);
        }
        SendEvLogImpl(1, rec);
    }

    void DeleteCommitedChunks() {
        auto& commited = Chunks[EChunkState::COMMITTED];
        NPDisk::TCommitRecord rec;
        rec.DeleteChunks = TVector<TChunkIdx>(commited.begin(), commited.end());
        SendEvLogImpl(1, rec);
        Chunks[EChunkState::DELETED].insert(commited.begin(), commited.end());
        commited.clear();
    }

    ui64 ReadLog(bool quietStopOnError = false, std::function<void(const NPDisk::TLogRecord&)> logResCallback = {}) {
        ui64 logRecordsRead = 0;

        NPDisk::TLogPosition position{0, 0};
        bool endOfLog = false;
        do {
            UNIT_ASSERT(PDiskParams);
            auto logReadRes = TestCtx->TestResponse<NPDisk::TEvReadLogResult>(
                new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound, position));

            if (logReadRes->Status != NKikimrProto::OK && quietStopOnError) {
                return logRecordsRead;
            }

            UNIT_ASSERT_EQUAL(NKikimrProto::OK, logReadRes->Status);
            UNIT_ASSERT(position == logReadRes->Position);
            for (const NPDisk::TLogRecord& rec : logReadRes->Results) {
                ++logRecordsRead;
                if (logResCallback) {
                    logResCallback(rec);
                }
                LastUsedLsn = Max(LastUsedLsn, rec.Lsn);
            }
            position = logReadRes->NextPosition;
            endOfLog = logReadRes->IsEndOfLog;
        } while (!endOfLog);

        return logRecordsRead;
    }

    void SendEvLogSync(const ui64 size = 128) {
        SendEvLogImpl(size, {}, false);
    }

    void CutLogAllButOne() {
        SendEvLogImpl(1, LastUsedLsn + 1, true);
    }

    ui64 OwnedLogRecords() const {
        return LastUsedLsn + 1 - FirstLsnToKeep;
    }

    void PerformHarakiri() {
        TestCtx->TestResponse<NPDisk::TEvHarakiriResult>(
            new NPDisk::TEvHarakiri(PDiskParams->Owner, PDiskParams->OwnerRound),
            NKikimrProto::OK);
    }

    void RespondToCutLog() {
        Cerr << __FILE__ << ":" << __LINE__ << Endl;
        THolder<NPDisk::TEvCutLog> evReq = TestCtx->Recv<NPDisk::TEvCutLog>();
        if (evReq) {
            CutLogAllButOne();
        }
    }

    void RespondToPreShredCompact(ui64 shredGeneration, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        THolder<NPDisk::TEvPreShredCompactVDisk> evReq = TestCtx->Recv<NPDisk::TEvPreShredCompactVDisk>();
        if (evReq) {
            TestCtx->Send(new NPDisk::TEvPreShredCompactVDiskResult(PDiskParams->Owner, PDiskParams->OwnerRound,
                shredGeneration, status, errorReason));
        }
    }

    void RespondToShred(ui64 shredGeneration, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        THolder<NPDisk::TEvShredVDisk> evReq = TestCtx->Recv<NPDisk::TEvShredVDisk>();
        if (evReq) {
            if (status == NKikimrProto::OK) {
                auto& commited = Chunks[EChunkState::COMMITTED];
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks = TVector<TChunkIdx>();
                for (const TChunkIdx &idx : evReq->ChunksToShred) {
                    if (commited.contains(idx)) {
                        rec.DeleteChunks.push_back(idx);
                        Chunks[EChunkState::DELETED].insert(idx);
                        commited.erase(idx);
                    }
                }
                SendEvLogImpl(1, rec);
            }
            TestCtx->Send(new NPDisk::TEvShredVDiskResult(PDiskParams->Owner, PDiskParams->OwnerRound,
                shredGeneration, status, errorReason));
        }
    }

private:
    void SendEvLogImpl(const ui64 size, TMaybe<NPDisk::TCommitRecord> commitRec) {
        auto evLog = MakeHolder<NPDisk::TEvLog>(PDiskParams->Owner, PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(size)),
                GetLsnSeg(), nullptr);

        if (commitRec) {
            evLog->Signature.SetCommitRecord();
            evLog->CommitRecord = std::move(*commitRec);
        }

        TestCtx->TestResponse<NPDisk::TEvLogResult>(evLog.Release(), NKikimrProto::OK);
    }

    void SendEvLogImpl(const ui64 size, TMaybe<ui64> firstLsnToKeep, bool isStartingPoint) {
        TMaybe<NPDisk::TCommitRecord> rec;

        if (firstLsnToKeep || isStartingPoint) {
            rec = NPDisk::TCommitRecord();
            rec->FirstLsnToKeep = firstLsnToKeep.GetOrElse(0);
            FirstLsnToKeep = Max(FirstLsnToKeep, firstLsnToKeep.GetOrElse(0));
            rec->IsStartingPoint = isStartingPoint;
        }
        SendEvLogImpl(size, rec);
    }
};

void TestChunkWriteReleaseRun(bool encryption);
}
