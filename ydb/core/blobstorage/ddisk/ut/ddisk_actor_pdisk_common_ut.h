#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/pdisk_io/aio.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/random/entropy.h>

#include <algorithm>
#include <cstring>
#include <numeric>
#include <random>

namespace NKikimr {
namespace {

using NKikimrBlobStorage::NDDisk::TReplyStatus;

constexpr ui32 MinBlockSize = 4096;
constexpr ui32 PDiskId = 1;
constexpr ui32 SlotId = 1;
constexpr ui32 ChunkSize = 16u << 20; // note that PDisk allocates slightly bigger chunk
constexpr ui32 ChunkCount = 1000;
constexpr ui64 DiskSize = (ui64)ChunkSize * ChunkCount;
constexpr ui64 DefaultPDiskSequence = 0x7e5700007e570000;

void FormatDisk(const TString& path, ui64 guid) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    EntropyPool().Read(&chunkKey, sizeof(chunkKey));
    EntropyPool().Read(&logKey, sizeof(logKey));
    EntropyPool().Read(&sysLogKey, sizeof(sysLogKey));

    TFormatOptions options;
    options.EnableSmallDiskOptimization = true;
    FormatPDisk(path, DiskSize, MinBlockSize, ChunkSize, guid,
        chunkKey, logKey, sysLogKey, DefaultPDiskSequence, "ddisk_pdisk_test", options);
}

struct TPDiskInfo {
    ui32 PDiskId;
    ui64 PDiskGuid;
};

struct TDiskInfo {
    TActorId DDiskServiceId;
    ui32 PDiskId;        // shorthand for PDisks[PDiskIdx].PDiskId
    ui32 SlotId;
    ui64 OwnerRound = 2;
    ui32 PDiskIdx = 0;   // index into TTestContext::PDisks
    ui32 GroupId = 0;    // unique per disk slot so PDisk treats them as distinct VDisks
};

class TTestContext {
    THolder<NActors::TTestActorRuntime> Runtime;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TTempDir TempDir;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NDDisk::TDDiskConfig DDiskConfig;
    NPDisk::TMainKey PDiskMainKey{.Keys = {DefaultPDiskSequence}, .IsInitialized = true};

public:
    TActorId Edge;
    TActorId DDiskServiceId;
    TVector<TPDiskInfo> PDisks;
    TVector<TDiskInfo> Disks;
    ui32 NodeId = 0;

    explicit TTestContext(NDDisk::TDDiskConfig ddiskConfig = {}, NLog::EPriority ddiskLogPriority = NLog::PRI_ERROR,
            ui32 numDisks = 1) {
        NActors::TTestActorRuntime::ResetFirstNodeId();
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        Runtime.Reset(new NActors::TTestActorRuntime(1, 1, true));

        auto appData = MakeHolder<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        appData->IoContextFactory = IoContext.get();

        Runtime->SetLogBackend(CreateStderrBackend());
        Runtime->Initialize(NActors::TTestActorRuntime::TEgg{appData.Release(), nullptr, {}, {}, {}});
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_ERROR);
        Runtime->SetLogPriority(NKikimrServices::BS_DDISK, ddiskLogPriority);

        Edge = Runtime->AllocateEdgeActor();
        NodeId = Runtime->GetNodeId(0);
        DDiskConfig = ddiskConfig;

        for (ui32 d = 0; d < numDisks; ++d) {
            AddDisk();
        }

        DDiskServiceId = Disks[0].DDiskServiceId;
    }

    // Formats a fresh on-disk file, registers a new PDisk actor for it, and returns its
    // index in PDisks. Does NOT attach a DDisk; use AddDDiskOnPDisk for that.
    ui32 AddPDisk() {
        const ui32 p = static_cast<ui32>(PDisks.size());
        const ui32 pdiskId = PDiskId + p;
        const ui64 pdiskGuid = 12345 + p;

        TString path = TempDir() + "/pdisk_" + ToString(p) + ".dat";
        {
            TFile file(path.c_str(), OpenAlways | RdWr);
            file.Resize(DiskSize);
            file.Close();
        }
        FormatDisk(path, pdiskGuid);

        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig(path, pdiskGuid, pdiskId, 0);
        pdiskConfig->ChunkSize = ChunkSize;
        pdiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(true);

        IActor* pdiskActor = CreatePDisk(pdiskConfig.Get(), PDiskMainKey, Counters);
        TActorId pdiskActorId = Runtime->Register(pdiskActor);

        TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);
        Runtime->RegisterService(pdiskServiceId, pdiskActorId);

        PDisks.push_back(TPDiskInfo{pdiskId, pdiskGuid});
        return p;
    }

    // Attaches a new DDisk slot on the given PDisk. The slot id auto-increments per-PDisk
    // (1, 2, 3, ...) and the new slot uses a fresh group id so PDisk treats it as a
    // distinct VDisk owner. Returns the new disk's index in Disks.
    ui32 AddDDiskOnPDisk(ui32 pdiskIdx) {
        ui32 slotId = SlotId;
        for (const auto& d : Disks) {
            if (d.PDiskIdx == pdiskIdx) {
                slotId = std::max(slotId, d.SlotId + 1);
            }
        }
        const ui32 d = static_cast<ui32>(Disks.size());
        Disks.push_back(TDiskInfo{
            .DDiskServiceId = {},
            .PDiskId = PDisks[pdiskIdx].PDiskId,
            .SlotId = slotId,
            .OwnerRound = 2,
            .PDiskIdx = pdiskIdx,
            .GroupId = d, // unique per disk slot
        });
        StartDDisk(d);
        return d;
    }

    // Adds a new PDisk + DDisk pair (1:1) and returns the new disk's index.
    ui32 AddDisk() {
        const ui32 pdiskIdx = AddPDisk();
        return AddDDiskOnPDisk(pdiskIdx);
    }

    void StartDDisk(ui32 diskIdx) {
        const auto& info = Disks[diskIdx];
        const ui32 pdiskId = info.PDiskId;
        const ui64 pdiskGuid = PDisks[info.PDiskIdx].PDiskGuid;
        const ui32 slotId = info.SlotId;
        const ui64 ownerRound = Disks[diskIdx].OwnerRound++;
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);

        TVector<TActorId> actorIds = {
            MakeBlobStorageDDiskId(NodeId, pdiskId, slotId),
        };
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone, ui32(1), ui32(1),
            ui32(1), &actorIds, TBlobStorageGroupInfo::EEM_ENC_V1, TBlobStorageGroupInfo::ELCP_IN_USE,
            TCypherKey((const ui8*)"TestKey", 8), TGroupId::FromValue(info.GroupId));

        TVDiskConfig::TBaseInfo baseInfo(
            TVDiskIdShort(groupInfo->GetVDiskId(0)),
            pdiskServiceId,
            pdiskGuid,
            pdiskId,
            NPDisk::DEVICE_TYPE_NVME,
            slotId,
            NKikimrBlobStorage::TVDiskKind::Default,
            ownerRound,
            "ddisk_pool");

        NDDisk::TPersistentBufferFormat pbFormat{256, 4, ChunkSize, 8};
        NDDisk::TDDiskConfig cfg = DDiskConfig;
        IActor* ddiskActor = NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), std::move(cfg), Counters);

        TActorId ddiskActorId = Runtime->Register(ddiskActor);
        TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, slotId);
        Runtime->RegisterService(ddiskServiceId, ddiskActorId);

        Disks[diskIdx].DDiskServiceId = ddiskServiceId;
    }

    void StopDDisk(ui32 diskIdx) {
        Runtime->Send(new IEventHandle(Disks[diskIdx].DDiskServiceId, Edge,
            new TEvents::TEvPoison()));
        Runtime->Send(new IEventHandle(Disks[diskIdx].DDiskServiceId, Edge,
            new NDDisk::TEvRead(), IEventHandle::FlagTrackDelivery));
        auto undelivered = Grab<TEvents::TEvUndelivered>();
        Y_ABORT_UNLESS(undelivered);
    }

    void RestartDDisk(ui32 diskIdx) {
        StopDDisk(diskIdx);
        StartDDisk(diskIdx);
        DDiskServiceId = Disks[0].DDiskServiceId;
    }

    // Restart the underlying PDisk (identified by a disk slot) while keeping the DDisk
    // actor running. Mirrors RestartPDiskSync in blobstorage_pdisk_ut_env.h: the PDisk
    // actor stays the same (same ActorId, so DDisk's BaseInfo.PDiskActorID remains valid),
    // but PDisk re-reads from disk and any in-memory reservations / owner rounds are gone.
    void RestartPDisk(ui32 diskIdx) {
        RestartPDiskByPDiskIdx(Disks[diskIdx].PDiskIdx);
    }

    void RestartPDiskByPDiskIdx(ui32 pdiskIdx) {
        const ui32 pdiskId = PDisks[pdiskIdx].PDiskId;
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);

        Runtime->Send(new IEventHandle(pdiskServiceId, Edge,
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr)));
        auto stopRes = Grab<NPDisk::TEvYardControlResult>();
        UNIT_ASSERT_VALUES_EQUAL(stopRes->Get()->Status, NKikimrProto::OK);

        Runtime->Send(new IEventHandle(pdiskServiceId, Edge,
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart,
                reinterpret_cast<void*>(&PDiskMainKey))));
        auto startRes = Grab<NPDisk::TEvYardControlResult>();
        UNIT_ASSERT_VALUES_EQUAL(startRes->Get()->Status, NKikimrProto::OK);
    }

    void ForceCutLog(ui32 diskIdx) {
        Runtime->Send(new IEventHandle(Disks[diskIdx].DDiskServiceId, Edge,
            new NPDisk::TEvCutLog(0, 0, Max<ui64>(), 0, 0, 0, 0)));
    }

    void Send(IEventBase* event, ui64 cookie = 0) {
        Runtime->Send(new IEventHandle(DDiskServiceId, Edge, event, 0, cookie));
    }

    void SendTo(ui32 diskIdx, IEventBase* event, ui64 cookie = 0) {
        Runtime->Send(new IEventHandle(Disks[diskIdx].DDiskServiceId, Edge, event, 0, cookie));
    }

    void DecreaseDispatchTimeout() {
        Runtime->SetDispatchTimeout(TDuration::Seconds(1));
    }

    template<typename TEvent>
    typename TEvent::TPtr Grab(TDuration timeout = TDuration::Seconds(30)) {
        return Runtime->GrabEdgeEventRethrow<TEvent>(Edge, timeout);
    }

    // Asserts that no event of the given type arrives at Edge within the timeout.
    // GrabEdgeEvent throws TEmptyEventQueueException when the event queue drains
    // before the timeout -- we treat that the same as "no matching event".
    template<typename TEvent>
    void ExpectNoReply(TDuration wait = TDuration::Seconds(1)) {
        try {
            auto reply = Runtime->GrabEdgeEvent<TEvent>(Edge, wait);
            UNIT_ASSERT_C(!reply, "Unexpected " << TypeName<TEvent>() << " reply");
        } catch (const NActors::TEmptyEventQueueException&) {
            // expected
        }
    }

    template<typename TEvent>
    typename TEvent::TPtr SendAndGrab(IEventBase* event, ui64 cookie = 0) {
        Send(event, cookie);
        return Grab<TEvent>();
    }

    template<typename TEvent>
    typename TEvent::TPtr SendToAndGrab(ui32 diskIdx, IEventBase* event, ui64 cookie = 0) {
        SendTo(diskIdx, event, cookie);
        return Grab<TEvent>();
    }
};

template<typename TResponseEvent>
void AssertStatus(const typename TResponseEvent::TPtr& ev, TReplyStatus::E status) {
    const auto& record = ev->Get()->Record;
    const auto actual = static_cast<TReplyStatus::E>(record.GetStatus());
    UNIT_ASSERT_C(actual == status, TStringBuilder()
        << "actual# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(actual)
        << " expected# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(status)
        << " errorReason# " << record.GetErrorReason());
}

TString MakeData(char ch, ui32 size) {
    TString data = TString::Uninitialized(size);
    memset(data.Detach(), ch, data.size());
    return data;
}

TRope MakeAlignedRope(const TString& data) {
    auto buf = TRcBuf::UninitializedPageAligned(data.size());
    std::memset(buf.GetDataMut(), 0, data.size());
    memcpy(buf.GetDataMut(), data.data(), data.size());
    return TRope(std::move(buf));
}

TString MakeDataWithIndex(ui32 idx, ui32 size) {
    TString data = TString::Uninitialized(size);
    ui32* ptr = reinterpret_cast<ui32*>(data.Detach());
    for (ui32 i = 0; i < size / sizeof(ui32); ++i) {
        ptr[i] = idx;
    }
    return data;
}

TString MakeDataWithTabletAndBlock(ui32 tabletId, ui32 blockIdx, ui32 size) {
    TString data = TString::Uninitialized(size);
    ui32* ptr = reinterpret_cast<ui32*>(data.Detach());
    for (ui32 i = 0; i < size / sizeof(ui32); i += 2) {
        ptr[i] = tabletId;
        ptr[i + 1] = blockIdx;
    }
    return data;
}

NDDisk::TQueryCredentials Connect(TTestContext& ctx, ui64 tabletId, ui32 generation) {
    NDDisk::TQueryCredentials creds;
    creds.TabletId = tabletId;
    creds.Generation = generation;

    auto connectResult = ctx.SendAndGrab<NDDisk::TEvConnectResult>(new NDDisk::TEvConnect(creds));
    AssertStatus<NDDisk::TEvConnectResult>(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

    return creds;
}

NDDisk::TQueryCredentials ConnectTo(TTestContext& ctx, ui32 diskIdx, ui64 tabletId, ui32 generation) {
    NDDisk::TQueryCredentials creds;
    creds.TabletId = tabletId;
    creds.Generation = generation;

    auto connectResult = ctx.SendToAndGrab<NDDisk::TEvConnectResult>(diskIdx, new NDDisk::TEvConnect(creds));
    AssertStatus<NDDisk::TEvConnectResult>(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

    return creds;
}

[[maybe_unused]] void TestWriteAndRead(NDDisk::TDDiskConfig ddiskConfig, ui32 blockSize) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

    const TString payload = MakeData('Q', 2 * blockSize);
    auto write = std::make_unique<NDDisk::TEvWrite>(creds,
        NDDisk::TBlockSelector(7, blockSize, static_cast<ui32>(payload.size())), NDDisk::TWriteInstruction(0));
    write->AddPayload(MakeAlignedRope(payload));

    auto writeResult = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release());
    AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);

    auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
        new NDDisk::TEvRead(creds, {7, blockSize, static_cast<ui32>(payload.size())}, {true}));
    AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

    const TString payload2 = MakeData('R', 2 * blockSize);
    const ui32 secondOffset = blockSize + static_cast<ui32>(payload.size());
    auto write2 = std::make_unique<NDDisk::TEvWrite>(creds,
        NDDisk::TBlockSelector(7, secondOffset, static_cast<ui32>(payload2.size())), NDDisk::TWriteInstruction(0));
    write2->AddPayload(MakeAlignedRope(payload2));

    auto writeResult2 = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write2.release());
    AssertStatus<NDDisk::TEvWriteResult>(writeResult2, TReplyStatus::OK);

    auto readResult2 = ctx.SendAndGrab<NDDisk::TEvReadResult>(
        new NDDisk::TEvRead(creds, {7, secondOffset, static_cast<ui32>(payload2.size())}, {true}));
    AssertStatus<NDDisk::TEvReadResult>(readResult2, TReplyStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(readResult2->Get()->GetPayload(0).ConvertToString(), payload2);
}

[[maybe_unused]] void TestCheckVChunksArePerTablet(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));

    const TString payload1 = MakeData('A', MinBlockSize);
    const TString payload2 = MakeData('B', MinBlockSize);

    NDDisk::TQueryCredentials creds1 = Connect(ctx, 101, 1);
    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds1, NDDisk::TBlockSelector(0, 0, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(payload1));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }

    {
        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds1, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload1);
    }

    NDDisk::TQueryCredentials creds2 = Connect(ctx, 102, 1);

    {
        auto rr = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds2, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr, TReplyStatus::OK);
        const TString data = rr->Get()->GetPayload(0).ConvertToString();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), MinBlockSize);
        UNIT_ASSERT(std::all_of(data.begin(), data.end(), [](char c) { return c == '\0'; }));
    }

    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds2, NDDisk::TBlockSelector(0, 0, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(payload2));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }

    {
        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds2, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload2);
    }

    {
        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds1, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload1);
    }
}

// writeSeed=0 means sequential writes, readSeed=0 means sequential reads.
// Non-zero seeds shuffle the respective order with std::mt19937.
[[maybe_unused]] void TestBatchWriteThenRead(NDDisk::TDDiskConfig ddiskConfig, ui32 totalWrites,
        ui32 writeSeed, ui32 readSeed, NLog::EPriority ddiskLogPriority = NLog::PRI_INFO) {
    TTestContext ctx(std::move(ddiskConfig), ddiskLogPriority);
    NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

    const ui32 blocksPerVChunk = ChunkSize / MinBlockSize;

    TVector<ui32> writeOrder(totalWrites);
    std::iota(writeOrder.begin(), writeOrder.end(), 0u);
    if (writeSeed) {
        std::mt19937 rng(writeSeed);
        std::shuffle(writeOrder.begin(), writeOrder.end(), rng);
    }

    for (ui32 idx : writeOrder) {
        ui64 vchunkIdx = idx / blocksPerVChunk;
        ui32 offset = (idx % blocksPerVChunk) * MinBlockSize;
        TString payload = MakeDataWithIndex(idx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(vchunkIdx, offset, MinBlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        ctx.Send(write.release());
    }

    for (ui32 i = 0; i < totalWrites; ++i) {
        auto writeResult = ctx.Grab<NDDisk::TEvWriteResult>();
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    }

    TVector<ui32> readOrder(totalWrites);
    std::iota(readOrder.begin(), readOrder.end(), 0u);
    if (readSeed) {
        std::mt19937 rng(readSeed);
        std::shuffle(readOrder.begin(), readOrder.end(), rng);
    }

    for (ui32 idx : readOrder) {
        ui64 vchunkIdx = idx / blocksPerVChunk;
        ui32 offset = (idx % blocksPerVChunk) * MinBlockSize;
        TString expected = MakeDataWithIndex(idx, MinBlockSize);

        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {vchunkIdx, offset, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), expected);
    }
}

[[maybe_unused]] void TestOverwrite(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

    const TString data1 = MakeData('A', MinBlockSize);
    auto w1 = std::make_unique<NDDisk::TEvWrite>(creds,
        NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
    w1->AddPayload(MakeAlignedRope(data1));
    auto r1 = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w1.release());
    AssertStatus<NDDisk::TEvWriteResult>(r1, TReplyStatus::OK);

    const TString data2 = MakeData('B', MinBlockSize);
    auto w2 = std::make_unique<NDDisk::TEvWrite>(creds,
        NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
    w2->AddPayload(MakeAlignedRope(data2));
    auto r2 = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w2.release());
    AssertStatus<NDDisk::TEvWriteResult>(r2, TReplyStatus::OK);

    auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
        new NDDisk::TEvRead(creds, {0, 0, MinBlockSize}, {true}));
    AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), data2);
}

[[maybe_unused]] void TestReadUnallocatedChunk(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

    auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
        new NDDisk::TEvRead(creds, {42, 0, 2 * MinBlockSize}, {true}));
    AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

    const TString data = readResult->Get()->GetPayload(0).ConvertToString();
    UNIT_ASSERT_VALUES_EQUAL(data.size(), 2 * MinBlockSize);
    UNIT_ASSERT(std::all_of(data.begin(), data.end(), [](char c) { return c == '\0'; }));
}

[[maybe_unused]] void TestManyVChunks(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

    constexpr ui32 numVChunks = 50;

    for (ui32 i = 0; i < numVChunks; ++i) {
        TString payload = MakeDataWithIndex(i, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(i, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        auto writeResult = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release());
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    }

    for (ui32 i = 0; i < numVChunks; ++i) {
        TString expected = MakeDataWithIndex(i, MinBlockSize);
        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {i, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), expected);
    }
}

[[maybe_unused]] void TestMultiTabletInterleaved(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds1 = Connect(ctx, 201, 1);
    NDDisk::TQueryCredentials creds2 = Connect(ctx, 202, 1);

    constexpr ui32 numBlocks = 16;

    for (ui32 i = 0; i < numBlocks; ++i) {
        TString payload1 = MakeDataWithIndex(i, MinBlockSize);
        auto w1 = std::make_unique<NDDisk::TEvWrite>(creds1,
            NDDisk::TBlockSelector(0, i * MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
        w1->AddPayload(MakeAlignedRope(payload1));
        auto wr1 = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w1.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr1, TReplyStatus::OK);

        TString payload2 = MakeDataWithIndex(i + 1000, MinBlockSize);
        auto w2 = std::make_unique<NDDisk::TEvWrite>(creds2,
            NDDisk::TBlockSelector(0, i * MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
        w2->AddPayload(MakeAlignedRope(payload2));
        auto wr2 = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w2.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr2, TReplyStatus::OK);
    }

    for (ui32 i = 0; i < numBlocks; ++i) {
        TString expected1 = MakeDataWithIndex(i, MinBlockSize);
        auto rr1 = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds1, {0, i * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr1, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(rr1->Get()->GetPayload(0).ConvertToString(), expected1);

        TString expected2 = MakeDataWithIndex(i + 1000, MinBlockSize);
        auto rr2 = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds2, {0, i * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr2, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(rr2->Get()->GetPayload(0).ConvertToString(), expected2);
    }
}

// 10 tablets, each writing across 5 vchunks. Data encodes tablet id and block index.
// writeSeed=0 means sequential writes; non-zero seeds shuffle the write order.
// withRestarts: restart DDisk after first 10 writes, after next 10, then after every 100.
// After the final read verification, restart and verify reads once more.
[[maybe_unused]] void TestBatchWriteThenReadMultiTabletInterleaved(NDDisk::TDDiskConfig ddiskConfig,
        ui32 writeSeed, NLog::EPriority ddiskLogPriority = NLog::PRI_INFO, bool withRestarts = false) {
    constexpr ui32 numTablets = 10;
    constexpr ui32 numVChunks = 5;
    constexpr ui32 blocksPerVChunk = ChunkSize / MinBlockSize;
    constexpr ui32 totalWrites = numTablets * numVChunks * blocksPerVChunk;
    constexpr ui32 baseTabletId = 301;
    constexpr ui32 MaxWritesInFlight = 1000;

    TTestContext ctx(std::move(ddiskConfig), ddiskLogPriority);

    TVector<NDDisk::TQueryCredentials> creds(numTablets);
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    struct WriteOp {
        ui32 tabletIdx;
        ui32 vchunkIdx;
        ui32 blockInChunk;
    };

    TVector<WriteOp> writeOps;
    writeOps.reserve(totalWrites);
    for (ui32 t = 0; t < numTablets; ++t) {
        for (ui32 v = 0; v < numVChunks; ++v) {
            for (ui32 b = 0; b < blocksPerVChunk; ++b) {
                writeOps.push_back({t, v, b});
            }
        }
    }

    if (writeSeed) {
        std::mt19937 rng(writeSeed);
        std::shuffle(writeOps.begin(), writeOps.end(), rng);
    }

    auto reconnect = [&]() {
        ctx.RestartDDisk(0);
        for (ui32 t = 0; t < numTablets; ++t) {
            creds[t] = Connect(ctx, baseTabletId + t, 1);
        }
    };

    auto sendWrite = [&](const WriteOp& op) {
        ui32 blockIdx = op.vchunkIdx * blocksPerVChunk + op.blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(baseTabletId + op.tabletIdx, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds[op.tabletIdx],
            NDDisk::TBlockSelector(op.vchunkIdx, op.blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        ctx.Send(write.release());
    };

    auto collectWriteResults = [&](ui32 count) {
        for (ui32 i = 0; i < count; ++i) {
            auto writeResult = ctx.Grab<NDDisk::TEvWriteResult>();
            AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
        }
    };

    ui32 inFlight = 0;

    if (withRestarts) {
        ui32 nextRestart = std::min(10u, totalWrites);

        for (ui32 i = 0; i < totalWrites; ++i) {
            if (inFlight >= MaxWritesInFlight) {
                collectWriteResults(1);
                --inFlight;
            }
            sendWrite(writeOps[i]);
            ++inFlight;

            if (i + 1 == nextRestart || i + 1 == totalWrites) {
                collectWriteResults(inFlight);
                inFlight = 0;

                if (i + 1 < totalWrites) {
                    reconnect();
                }

                if (nextRestart == 10) {
                    nextRestart = std::min(20u, totalWrites);
                } else {
                    nextRestart = std::min(nextRestart + 100, totalWrites);
                }
            }
        }
    } else {
        for (const auto& op : writeOps) {
            if (inFlight >= MaxWritesInFlight) {
                collectWriteResults(1);
                --inFlight;
            }
            sendWrite(op);
            ++inFlight;
        }
        collectWriteResults(inFlight);
    }

    auto verifyAllReads = [&]() {
        for (ui32 t = 0; t < numTablets; ++t) {
            ui32 tabletId = baseTabletId + t;
            for (ui32 v = 0; v < numVChunks; ++v) {
                for (ui32 b = 0; b < blocksPerVChunk; ++b) {
                    ui32 blockIdx = v * blocksPerVChunk + b;
                    TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

                    auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
                        new NDDisk::TEvRead(creds[t], {v, b * MinBlockSize, MinBlockSize}, {true}));
                    AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

                    const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
                    ui32 readTabletId = 0;
                    std::memcpy(&readTabletId, actual.data(), sizeof(readTabletId));
                    UNIT_ASSERT_VALUES_EQUAL_C(readTabletId, tabletId,
                        "tablet id mismatch at tablet=" << tabletId << " vchunk=" << v << " block=" << b);
                    UNIT_ASSERT_VALUES_EQUAL(actual, expected);
                }
            }
        }
    };

    verifyAllReads();

    if (withRestarts) {
        reconnect();
        verifyAllReads();
    }
}

[[maybe_unused]] void TestMultiTabletInterleavedWritesWithDDiskRestart(NDDisk::TDDiskConfig ddiskConfig) {
    constexpr ui32 numTablets = 4;
    constexpr ui32 baseTabletId = 201;

    TTestContext ctx(std::move(ddiskConfig));

    auto writeBlock = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        auto writeResult = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release());
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    };

    auto readAndVerify = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

        const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
        ui32 readTabletId = 0;
        std::memcpy(&readTabletId, actual.data(), sizeof(readTabletId));
        UNIT_ASSERT_VALUES_EQUAL_C(readTabletId, tabletId,
            "tablet id mismatch at tablet=" << tabletId << " vchunk=" << vchunkIdx << " block=" << blockInChunk);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    };

    NDDisk::TQueryCredentials creds[numTablets];
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    // Phase 1: each tablet writes block 0 of vchunk 0 (tablet1, tablet2 order)
    for (ui32 t = 0; t < numTablets; ++t) {
        writeBlock(creds[t], baseTabletId + t, 0, 0);
    }

    // Phase 2: restart DDisk, reconnect
    ctx.RestartDDisk(0);
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    // Phase 3: each tablet writes block 1 of vchunk 0 in reverse order (tablet2, tablet1)
    for (ui32 t = numTablets; t-- > 0; ) {
        writeBlock(creds[t], baseTabletId + t, 0, 1);
    }

    // Phase 4: both tablets write block 0 of vchunks 1, 2, 3
    for (ui32 t = 0; t < numTablets; ++t) {
        for (ui32 v = 1; v <= 3; ++v) {
            writeBlock(creds[t], baseTabletId + t, v, 0);
        }
    }

    // Phase 5: read all written data and verify
    for (ui32 t = 0; t < numTablets; ++t) {
        ui32 tabletId = baseTabletId + t;
        readAndVerify(creds[t], tabletId, 0, 0);
        readAndVerify(creds[t], tabletId, 0, 1);
        for (ui32 v = 1; v <= 3; ++v) {
            readAndVerify(creds[t], tabletId, v, 0);
        }
    }
}

[[maybe_unused]] void TestMultipleRestarts(NDDisk::TDDiskConfig ddiskConfig) {
    constexpr ui32 numTablets = 2;
    constexpr ui32 baseTabletId = 501;
    constexpr ui32 numRestarts = 3;

    TTestContext ctx(std::move(ddiskConfig));

    auto writeBlock = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        auto writeResult = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release());
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    };

    auto readAndVerify = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

        const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
        ui32 readTabletId = 0;
        std::memcpy(&readTabletId, actual.data(), sizeof(readTabletId));
        UNIT_ASSERT_VALUES_EQUAL_C(readTabletId, tabletId,
            "tablet id mismatch at tablet=" << tabletId << " vchunk=" << vchunkIdx << " block=" << blockInChunk);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    };

    NDDisk::TQueryCredentials creds[numTablets];
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    for (ui32 t = 0; t < numTablets; ++t) {
        writeBlock(creds[t], baseTabletId + t, 0, 0);
    }

    for (ui32 restart = 0; restart < numRestarts; ++restart) {
        ctx.RestartDDisk(0);
        for (ui32 t = 0; t < numTablets; ++t) {
            creds[t] = Connect(ctx, baseTabletId + t, 1);
        }
        ui32 vchunk = restart + 1;
        for (ui32 t = 0; t < numTablets; ++t) {
            writeBlock(creds[t], baseTabletId + t, vchunk, 0);
        }
    }

    for (ui32 t = 0; t < numTablets; ++t) {
        ui32 tabletId = baseTabletId + t;
        for (ui32 v = 0; v <= numRestarts; ++v) {
            readAndVerify(creds[t], tabletId, v, 0);
        }
    }
}

[[maybe_unused]] void TestOverwriteAfterRestart(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    NDDisk::TQueryCredentials creds = Connect(ctx, 601, 1);

    const TString dataA = MakeData('A', MinBlockSize);
    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(dataA));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }

    ctx.RestartDDisk(0);
    creds = Connect(ctx, 601, 1);

    const TString dataB = MakeData('B', MinBlockSize);
    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(dataB));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }

    {
        auto rr = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(rr->Get()->GetPayload(0).ConvertToString(), dataB);
    }

    const TString dataC = MakeData('C', MinBlockSize);
    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(dataC));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }
    {
        auto rr = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {0, MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(rr->Get()->GetPayload(0).ConvertToString(), dataC);
    }
}

[[maybe_unused]] void TestEmptyRestart(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));
    Connect(ctx, 701, 1);

    ctx.RestartDDisk(0);
    NDDisk::TQueryCredentials creds = Connect(ctx, 701, 1);

    const TString data = MakeData('X', MinBlockSize);
    {
        auto w = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        w->AddPayload(MakeAlignedRope(data));
        auto wr = ctx.SendAndGrab<NDDisk::TEvWriteResult>(w.release());
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }
    {
        auto rr = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {0, 0, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(rr, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(rr->Get()->GetPayload(0).ConvertToString(), data);
    }
}

[[maybe_unused]] void TestRestartAfterCutLog(NDDisk::TDDiskConfig ddiskConfig) {
    constexpr ui32 numTablets = 3;
    constexpr ui32 baseTabletId = 801;

    TTestContext ctx(std::move(ddiskConfig));

    auto writeBlock = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 4 + blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        auto writeResult = ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release());
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    };

    auto readAndVerify = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId, ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 4 + blockInChunk;
        TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

        auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

        const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
        ui32 readTabletId = 0;
        std::memcpy(&readTabletId, actual.data(), sizeof(readTabletId));
        UNIT_ASSERT_VALUES_EQUAL_C(readTabletId, tabletId,
            "tablet id mismatch at tablet=" << tabletId << " vchunk=" << vchunkIdx << " block=" << blockInChunk);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    };

    NDDisk::TQueryCredentials creds[numTablets];
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    // Phase 1: write to vchunks 0 and 1 for each tablet
    for (ui32 t = 0; t < numTablets; ++t) {
        for (ui32 v = 0; v < 2; ++v) {
            writeBlock(creds[t], baseTabletId + t, v, 0);
        }
    }

    // Phase 2: force CutLog to rewrite snapshot, then flush via a write
    ctx.ForceCutLog(0);
    writeBlock(creds[0], baseTabletId, 0, 1);

    // Phase 3: allocate more chunks after the rewritten snapshot
    for (ui32 t = 0; t < numTablets; ++t) {
        for (ui32 v = 2; v < 4; ++v) {
            writeBlock(creds[t], baseTabletId + t, v, 0);
        }
    }

    // Phase 4: restart and reconnect
    ctx.RestartDDisk(0);
    for (ui32 t = 0; t < numTablets; ++t) {
        creds[t] = Connect(ctx, baseTabletId + t, 1);
    }

    // Phase 5: read all data and verify
    for (ui32 t = 0; t < numTablets; ++t) {
        ui32 tabletId = baseTabletId + t;
        readAndVerify(creds[t], tabletId, 0, 0);
        for (ui32 v = 1; v < 4; ++v) {
            readAndVerify(creds[t], tabletId, v, 0);
        }
    }
    readAndVerify(creds[0], baseTabletId, 0, 1);
}

[[maybe_unused]] void TestReadWithoutConnect(NDDisk::TDDiskConfig ddiskConfig) {
    TTestContext ctx(std::move(ddiskConfig));

    NDDisk::TQueryCredentials creds;
    creds.TabletId = 30;
    creds.Generation = 1;

    auto readResult = ctx.SendAndGrab<NDDisk::TEvReadResult>(
        new NDDisk::TEvRead(creds, {0, 0, MinBlockSize}, {true}));
    AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::SESSION_MISMATCH);
}

// Write data to DDisk0, sync to DDisk1 via TEvSyncWithDDisk, then read and verify on DDisk1.
// segmentsPerSync controls how many non-overlapping segments each sync request carries.
[[maybe_unused]] void TestSyncWithDDisk(ui32 numTablets, ui32 numVChunks, ui32 blocksPerVChunk,
        ui32 segmentsPerSync, NLog::EPriority ddiskLogPriority = NLog::PRI_ERROR) {
    TTestContext ctx({}, ddiskLogPriority, 2);
    const ui32 baseTabletId = 401;

    struct TabletCreds {
        NDDisk::TQueryCredentials Src;
        NDDisk::TQueryCredentials Dst;
    };
    TVector<TabletCreds> tablets(numTablets);
    for (ui32 t = 0; t < numTablets; ++t) {
        tablets[t].Src = ConnectTo(ctx, 0, baseTabletId + t, 1);
        tablets[t].Dst = ConnectTo(ctx, 1, baseTabletId + t, 1);
    }

    // Phase 1: batch-write to DDisk0
    ui32 totalWrites = numTablets * numVChunks * blocksPerVChunk;
    for (ui32 t = 0; t < numTablets; ++t) {
        for (ui32 v = 0; v < numVChunks; ++v) {
            for (ui32 b = 0; b < blocksPerVChunk; ++b) {
                ui32 blockIdx = v * blocksPerVChunk + b;
                TString payload = MakeDataWithTabletAndBlock(baseTabletId + t, blockIdx, MinBlockSize);
                auto write = std::make_unique<NDDisk::TEvWrite>(tablets[t].Src,
                    NDDisk::TBlockSelector(v, b * MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
                write->AddPayload(MakeAlignedRope(payload));
                ctx.SendTo(0, write.release());
            }
        }
    }
    for (ui32 i = 0; i < totalWrites; ++i) {
        auto wr = ctx.Grab<NDDisk::TEvWriteResult>();
        AssertStatus<NDDisk::TEvWriteResult>(wr, TReplyStatus::OK);
    }

    // Phase 2: sync DDisk0 -> DDisk1
    ui32 totalSyncs = 0;
    const auto srcDDiskId = std::make_tuple(ctx.NodeId, ctx.Disks[0].PDiskId, ctx.Disks[0].SlotId);
    for (ui32 t = 0; t < numTablets; ++t) {
        const ui64 srcGuid = *tablets[t].Src.DDiskInstanceGuid;
        for (ui32 v = 0; v < numVChunks; ++v) {
            const ui32 totalBlocks = blocksPerVChunk;
            const ui32 blocksPerSegment = (totalBlocks + segmentsPerSync - 1) / segmentsPerSync;

            auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
                tablets[t].Dst, srcDDiskId, std::optional<ui64>(srcGuid));

            for (ui32 s = 0; s < segmentsPerSync; ++s) {
                ui32 startBlock = s * blocksPerSegment;
                ui32 endBlock = std::min(startBlock + blocksPerSegment, totalBlocks);
                if (startBlock >= endBlock) {
                    break;
                }
                syncEv->AddSegment(NDDisk::TBlockSelector(v,
                    startBlock * MinBlockSize, (endBlock - startBlock) * MinBlockSize));
            }
            ctx.SendTo(1, syncEv.release());
            totalSyncs++;
        }
    }
    for (ui32 i = 0; i < totalSyncs; ++i) {
        auto syncResult = ctx.Grab<NDDisk::TEvSyncWithDDiskResult>();
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(syncResult, TReplyStatus::OK);
    }

    // Phase 3: read from DDisk1 and verify
    for (ui32 t = 0; t < numTablets; ++t) {
        ui32 tabletId = baseTabletId + t;
        for (ui32 v = 0; v < numVChunks; ++v) {
            for (ui32 b = 0; b < blocksPerVChunk; ++b) {
                ui32 blockIdx = v * blocksPerVChunk + b;
                TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

                auto readResult = ctx.SendToAndGrab<NDDisk::TEvReadResult>(1,
                    new NDDisk::TEvRead(tablets[t].Dst, {v, b * MinBlockSize, MinBlockSize}, {true}));
                AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

                const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
                ui32 readValue = 0;
                std::memcpy(&readValue, actual.data(), sizeof(readValue));
                UNIT_ASSERT_VALUES_EQUAL_C(readValue, tabletId,
                    "tablet id mismatch at tablet=" << tabletId << " vchunk=" << v << " block=" << b);
                UNIT_ASSERT_VALUES_EQUAL(actual, expected);
            }
        }
    }
}

// Verifies behaviour after a PDisk restart -- both with and without restarting DDisk.
//
// Common setup: tablets 1 and 2 commit a chunk on DDisk slot 0 (which is on PDisk 0),
// then PDisk 0 is restarted in place. After that a fresh DDisk slot is added on the
// SAME PDisk 0 (different VDisk owner, different SlotId) and tablet 3 writes to it --
// this proves the restarted PDisk is functional through a fresh owner.
//
// Variant restartDDisk == false (zombie):
//   The original DDisk slot 0 keeps running. Its OwnerRound is now stale, so the first
//   reply it gets back from PDisk for any owner-stamped request will be INVALID_ROUND,
//   and CheckPDiskReply switches it to StateFuncTerminate. From that point client
//   requests are silently dropped (no reply). We attempt vchunk 0 page 1 writes
//   (uring mode bypasses PDisk for raw writes, PDisk fallback hits TEvChunkWriteRaw and
//   zombifies immediately) and then vchunk 1 page 0 writes (chunk reservation always
//   goes through PDisk, so this is guaranteed to zombify and produce no reply in
//   either uring or PDisk-fallback mode). The test must NOT crash.
//
// Variant restartDDisk == true (warden-style recovery):
//   After the PDisk restart we also restart DDisk slot 0; the new DDisk instance uses
//   the next OwnerRound and rebuilds its chunk map from the on-disk log. Tablets 1 and
//   2 reconnect, write fresh vchunks, and we read everything back to confirm pre- and
//   post-restart data survived.
[[maybe_unused]] void TestPDiskRestartWithReservedChunks(NDDisk::TDDiskConfig ddiskConfig,
        bool restartDDisk) {
    constexpr ui32 baseTabletId = 901;

    TTestContext ctx(std::move(ddiskConfig));

    auto makeWrite = [&](const NDDisk::TQueryCredentials& creds, ui32 tabletId,
            ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        return write;
    };

    auto writeBlock = [&](ui32 diskIdx, const NDDisk::TQueryCredentials& creds, ui32 tabletId,
            ui32 vchunkIdx, ui32 blockInChunk) {
        auto writeResult = ctx.SendToAndGrab<NDDisk::TEvWriteResult>(diskIdx,
            makeWrite(creds, tabletId, vchunkIdx, blockInChunk).release());
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    };

    auto readAndVerify = [&](ui32 diskIdx, const NDDisk::TQueryCredentials& creds, ui32 tabletId,
            ui32 vchunkIdx, ui32 blockInChunk) {
        ui32 blockIdx = vchunkIdx * 2 + blockInChunk;
        TString expected = MakeDataWithTabletAndBlock(tabletId, blockIdx, MinBlockSize);

        auto readResult = ctx.SendToAndGrab<NDDisk::TEvReadResult>(diskIdx,
            new NDDisk::TEvRead(creds, {vchunkIdx, blockInChunk * MinBlockSize, MinBlockSize}, {true}));
        AssertStatus<NDDisk::TEvReadResult>(readResult, TReplyStatus::OK);

        const TString actual = readResult->Get()->GetPayload(0).ConvertToString();
        ui32 readTabletId = 0;
        std::memcpy(&readTabletId, actual.data(), sizeof(readTabletId));
        UNIT_ASSERT_VALUES_EQUAL_C(readTabletId, tabletId,
            "tablet id mismatch at disk=" << diskIdx << " tablet=" << tabletId
            << " vchunk=" << vchunkIdx << " block=" << blockInChunk);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    };

    NDDisk::TQueryCredentials creds1 = ConnectTo(ctx, 0, baseTabletId + 0, 1);
    NDDisk::TQueryCredentials creds2 = ConnectTo(ctx, 0, baseTabletId + 1, 1);

    // Phase 1: tablet1 and tablet2 each write 1 page to vchunk 0 on DDisk slot 0
    // (commits a chunk on PDisk 0).
    writeBlock(0, creds1, baseTabletId + 0, 0, 0);
    writeBlock(0, creds2, baseTabletId + 1, 0, 0);

    // Phase 2: restart PDisk 0 in place. DDisk slot 0 is still alive but its owner
    // round and reserved chunks are stale relative to the freshly-rebuilt PDisk state.
    ctx.RestartPDisk(0);

    if (restartDDisk) {
        // Warden-style: replace the DDisk actor for slot 0. The new instance YardInits
        // with the next OwnerRound and recovers its chunk map from PDisk's log.
        ctx.RestartDDisk(0);
    }

    // Phase 3: add a fresh DDisk slot on the SAME PDisk 0 (new owner, different SlotId)
    // and have tablet 3 write to it -- this proves the restarted PDisk works through a
    // fresh owner regardless of what happened to DDisk slot 0.
    const ui32 disk2Idx = ctx.AddDDiskOnPDisk(0);
    NDDisk::TQueryCredentials creds3 = ConnectTo(ctx, disk2Idx, baseTabletId + 2, 1);
    writeBlock(disk2Idx, creds3, baseTabletId + 2, 0, 0);

    if (restartDDisk) {
        // Reconnect tablets 1 and 2 to the new DDisk slot 0 actor (their old credentials
        // belonged to the previous instance that PassAway'd during RestartDDisk).
        creds1 = ConnectTo(ctx, 0, baseTabletId + 0, 1);
        creds2 = ConnectTo(ctx, 0, baseTabletId + 1, 1);

        // Tablets 1 and 2 write to a brand-new vchunk on slot 0 (forces fresh chunk
        // allocation through the post-restart PDisk + DDisk).
        writeBlock(0, creds1, baseTabletId + 0, 1, 0);
        writeBlock(0, creds2, baseTabletId + 1, 1, 0);

        // Read everything back: pre- and post-restart writes on both slots.
        readAndVerify(0, creds1, baseTabletId + 0, 0, 0);
        readAndVerify(0, creds2, baseTabletId + 1, 0, 0);
        readAndVerify(0, creds1, baseTabletId + 0, 1, 0);
        readAndVerify(0, creds2, baseTabletId + 1, 1, 0);
        readAndVerify(disk2Idx, creds3, baseTabletId + 2, 0, 0);
        return;
    }

    // Zombie variant: DDisk slot 0 is still the stale instance. Its OwnerRound is
    // stale so any request that goes through PDisk (chunk reserve, log) gets
    // INVALID_ROUND and CheckPDiskReply switches DDisk to StateFuncTerminate.
    // After that the actor silently drops all further messages.
    //
    // Writes to vchunk 0 page 1 succeed in both modes: the chunk is already
    // committed, so PDisk does not check OwnerRound for raw chunk I/O.
    writeBlock(0, creds1, baseTabletId + 0, 0, 1);
    writeBlock(0, creds2, baseTabletId + 1, 0, 1);

    // Writes to vchunk 1 need a fresh chunk reservation that goes through PDisk,
    // hits INVALID_ROUND, and DDisk zombifies -- no reply in either mode.

    ctx.DecreaseDispatchTimeout();

    ctx.SendTo(0, makeWrite(creds1, baseTabletId + 0, 1, 0).release());
    ctx.ExpectNoReply<NDDisk::TEvWriteResult>();

    ctx.SendTo(0, makeWrite(creds2, baseTabletId + 1, 1, 0).release());
    ctx.ExpectNoReply<NDDisk::TEvWriteResult>();

    // Tablet 3's writes on the new DDisk slot are still readable -- proves the
    // restarted PDisk is functional and the zombie DDisk slot didn't corrupt it.
    readAndVerify(disk2Idx, creds3, baseTabletId + 2, 0, 0);
}

} // anonymous namespace
} // NKikimr
