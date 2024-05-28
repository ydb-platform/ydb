#pragma once

#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_defs.h"
#include "blobstorage_pdisk_data.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

struct TActorTestContext {
    using EDiskMode = NPDisk::NSectorMap::EDiskMode;
public:
    struct TSettings {
        bool IsBad;
        bool UsePDiskMock = false;
        ui64 DiskSize = 0;
        EDiskMode DiskMode = EDiskMode::DM_NONE;
        ui32 ChunkSize = 128 * (1 << 20);
        bool SmallDisk = false;
        bool SuppressCompatibilityCheck = false;
    };

private:
    std::optional<TActorId> PDiskActor;
    THolder<TTestActorRuntime> Runtime;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    NPDisk::TPDisk *PDisk = nullptr;

public:
    TActorId Sender;
    NPDisk::TMainKey MainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence }, .IsInitialized = true };
    TTestContext TestCtx;
    TSettings Settings;

    TIntrusivePtr<TPDiskConfig> DefaultPDiskConfig(bool isBad) {
        TString path;
        EntropyPool().Read(&TestCtx.PDiskGuid, sizeof(TestCtx.PDiskGuid));
        ui64 formatGuid = TestCtx.PDiskGuid + static_cast<ui64>(isBad);
        if (Settings.DiskSize) {
            FormatPDiskForTest(path, formatGuid, Settings.ChunkSize, Settings.DiskSize, false, TestCtx.SectorMap, Settings.SmallDisk);
        } else {
            FormatPDiskForTest(path, formatGuid, Settings.ChunkSize, false, TestCtx.SectorMap, Settings.SmallDisk);
        }

        ui64 pDiskCategory = 0;
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(path, TestCtx.PDiskGuid, 1, pDiskCategory);
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->ChunkSize = Settings.ChunkSize;
        pDiskConfig->SectorMap = TestCtx.SectorMap;
        pDiskConfig->EnableSectorEncryption = !pDiskConfig->SectorMap;
        pDiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(Settings.SmallDisk);
        pDiskConfig->FeatureFlags.SetSuppressCompatibilityCheck(Settings.SuppressCompatibilityCheck);
        return pDiskConfig;
    }

    TActorTestContext(TSettings settings)
        : Runtime(new TTestActorRuntime(1, true))
        , TestCtx(false, true, settings.DiskMode, settings.DiskSize)
        , Settings(settings)
    {
        auto appData = MakeHolder<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        appData->IoContextFactory = IoContext.get();

        Runtime->SetLogBackend(IsLowVerbose ? CreateStderrBackend() : CreateNullBackend());
        Runtime->Initialize(TTestActorRuntime::TEgg{appData.Release(), nullptr, {}, {}});
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_NOTICE);
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK_SYSLOG, NLog::PRI_NOTICE);
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK_TEST, NLog::PRI_DEBUG);
        Sender = Runtime->AllocateEdgeActor();

        TIntrusivePtr<TPDiskConfig> cfg = DefaultPDiskConfig(Settings.IsBad);
        UpdateConfigRecreatePDisk(cfg);
    }

    TIntrusivePtr<TPDiskConfig> GetPDiskConfig() {
        if (!Settings.UsePDiskMock) {
            return GetPDisk()->Cfg;
        }
        return nullptr;
    }

    void UpdateConfigRecreatePDisk(TIntrusivePtr<TPDiskConfig> cfg) {
        if (PDiskActor) {
            TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                    NKikimrProto::OK);
            PDisk = nullptr;
            Runtime->Send(new IEventHandle(*PDiskActor, Sender, new TKikimrEvents::TEvPoisonPill));
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
        Runtime->Send(new IEventHandle(*PDiskActor, Sender, ev));
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

            PDiskActor = PDisk->PDiskActor;
        }
        return PDisk;
    }
    
    void StartPDiskRestart() {
        ui32 pdiskId = GetPDisk()->PDiskId;

        Send(new TEvBlobStorage::TEvAskWardenRestartPDiskResult(pdiskId, MainKey, true, nullptr));
        const auto evInitRes = Recv<TEvBlobStorage::TEvNotifyWardenPDiskRestarted>();

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
            UNIT_ASSERT_C(evRes->Status == status.value(), evRes->ToString());
        }

        UNIT_ASSERT(evRes->Status == NKikimrProto::OK || !evRes->ErrorReason.empty());

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

    TVDiskMock(TActorTestContext *testCtx)
        : TestCtx(testCtx)
        , VDiskID(Idx.fetch_add(1), 1, 0, 0, 0)
    {}

    TLsnSeg GetLsnSeg() {
        ++LastUsedLsn;
        return {LastUsedLsn, LastUsedLsn};
    };

    void InitFull() {
        Init();
        ReadLog();
        SendEvLogImpl(1, {}, true);
    }

    void Init() {
        const auto evInitRes = TestCtx->TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(OwnerRound.fetch_add(1), VDiskID, TestCtx->TestCtx.PDiskGuid),
                NKikimrProto::OK);
        PDiskParams = evInitRes->PDiskParams;

        TSet<TChunkIdx> commited = Chunks[EChunkState::COMMITTED];
        for (TChunkIdx chunk : evInitRes->OwnedChunks) {
            UNIT_ASSERT_C(commited.count(chunk), "misowned chunk# " << chunk);
            commited.erase(chunk);
        }
        UNIT_ASSERT_C(commited.empty(), "there are leaked chunks# " << FormatList(commited));
    }


    void ReserveChunk() {
        const auto evReserveRes = TestCtx->TestResponse<NPDisk::TEvChunkReserveResult>(
                new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound, 1),
                NKikimrProto::OK);
        UNIT_ASSERT(evReserveRes->ChunkIds.size() == 1);
        const ui32 reservedChunk = evReserveRes->ChunkIds.front();
        Chunks[EChunkState::RESERVED].emplace(reservedChunk);
    }

    void CommitReservedChunks() {
        auto& reservedChunks = Chunks[EChunkState::RESERVED];
        NPDisk::TCommitRecord rec;
        rec.CommitChunks = TVector<TChunkIdx>(reservedChunks.begin(), reservedChunks.end());
        SendEvLogImpl(1, rec);
        Chunks[EChunkState::COMMITTED].insert(reservedChunks.begin(), reservedChunks.end());
        reservedChunks.clear();
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

void TestChunkWriteReleaseRun();
}
