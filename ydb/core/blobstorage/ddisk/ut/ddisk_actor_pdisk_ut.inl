#include <library/cpp/testing/unittest/registar.h>

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

struct TDiskInfo {
    TActorId DDiskServiceId;
    ui32 PDiskId;
    ui32 SlotId;
    ui64 OwnerRound = 2;
};

class TTestContext {
    THolder<NActors::TTestActorRuntime> Runtime;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TTempDir TempDir;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NDDisk::TDDiskConfig DDiskConfig;

public:
    TActorId Edge;
    TActorId DDiskServiceId;
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
            const ui32 pdiskId = PDiskId + d;
            const ui64 pdiskGuid = 12345 + d;

            TString path = TempDir() + "/pdisk_" + ToString(d) + ".dat";
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

            NPDisk::TMainKey mainKey{.Keys = {DefaultPDiskSequence}, .IsInitialized = true};
            IActor* pdiskActor = CreatePDisk(pdiskConfig.Get(), mainKey, Counters);
            TActorId pdiskActorId = Runtime->Register(pdiskActor);

            TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);
            Runtime->RegisterService(pdiskServiceId, pdiskActorId);

            Disks.push_back(TDiskInfo{{}, pdiskId, SlotId});
            StartDDisk(d);
        }

        DDiskServiceId = Disks[0].DDiskServiceId;
    }

    void StartDDisk(ui32 diskIdx) {
        const ui32 pdiskId = Disks[diskIdx].PDiskId;
        const ui64 pdiskGuid = 12345 + diskIdx;
        const ui64 ownerRound = Disks[diskIdx].OwnerRound++;
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);

        TVector<TActorId> actorIds = {
            MakeBlobStorageDDiskId(NodeId, pdiskId, SlotId),
        };
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone, ui32(1), ui32(1),
            ui32(1), &actorIds);

        TVDiskConfig::TBaseInfo baseInfo(
            TVDiskIdShort(groupInfo->GetVDiskId(0)),
            pdiskServiceId,
            pdiskGuid,
            pdiskId,
            NPDisk::DEVICE_TYPE_NVME,
            SlotId,
            NKikimrBlobStorage::TVDiskKind::Default,
            ownerRound,
            "ddisk_pool");

        NDDisk::TPersistentBufferFormat pbFormat{256, 4, ChunkSize, 8};
        NDDisk::TDDiskConfig cfg = DDiskConfig;
        IActor* ddiskActor = NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), std::move(cfg), Counters);

        TActorId ddiskActorId = Runtime->Register(ddiskActor);
        TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, SlotId);
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

    template<typename TEvent>
    typename TEvent::TPtr Grab(TDuration timeout = TDuration::Seconds(30)) {
        return Runtime->GrabEdgeEventRethrow<TEvent>(Edge, timeout);
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

} // anonymous namespace
