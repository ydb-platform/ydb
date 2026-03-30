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
constexpr ui32 ChunkSize = 16u << 20;
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

class TTestContext {
    THolder<NActors::TTestActorRuntime> Runtime;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TTempDir TempDir;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    TActorId Edge;
    TActorId DDiskServiceId;

    explicit TTestContext(NDDisk::TDDiskConfig ddiskConfig = {}, NLog::EPriority ddiskLogPriority = NLog::PRI_ERROR) {
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

        TString path = TempDir() + "/pdisk.dat";
        {
            TFile file(path.c_str(), OpenAlways | RdWr);
            file.Resize(DiskSize);
            file.Close();
        }

        const ui64 pdiskGuid = 12345;
        FormatDisk(path, pdiskGuid);

        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig(path, pdiskGuid, PDiskId, 0);
        pdiskConfig->ChunkSize = ChunkSize;
        pdiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(true);

        NPDisk::TMainKey mainKey{.Keys = {DefaultPDiskSequence}, .IsInitialized = true};
        IActor* pdiskActor = CreatePDisk(pdiskConfig.Get(), mainKey, Counters);
        TActorId pdiskActorId = Runtime->Register(pdiskActor);
        const ui32 nodeId = Runtime->GetNodeId(0);

        TActorId pdiskServiceId = MakeBlobStoragePDiskID(nodeId, PDiskId);
        Runtime->RegisterService(pdiskServiceId, pdiskActorId);

        TVector<TActorId> actorIds = {
            MakeBlobStorageDDiskId(nodeId, PDiskId, SlotId),
        };
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone, ui32(1), ui32(1),
            ui32(1), &actorIds);

        TVDiskConfig::TBaseInfo baseInfo(
            TVDiskIdShort(groupInfo->GetVDiskId(0)),
            pdiskServiceId,
            pdiskGuid,
            PDiskId,
            NPDisk::DEVICE_TYPE_NVME,
            SlotId,
            NKikimrBlobStorage::TVDiskKind::Default,
            2,
            "ddisk_pool");

        // TODO: remove when DDisk and PB are separated
        NDDisk::TPersistentBufferFormat pbFormat{256, 4, ChunkSize, 8};
        IActor* ddiskActor = NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), std::move(ddiskConfig), Counters);

        TActorId ddiskActorId = Runtime->Register(ddiskActor);
        DDiskServiceId = MakeBlobStorageDDiskId(nodeId, PDiskId, SlotId);
        Runtime->RegisterService(DDiskServiceId, ddiskActorId);
    }

    void Send(IEventBase* event, ui64 cookie = 0) {
        Runtime->Send(new IEventHandle(DDiskServiceId, Edge, event, 0, cookie));
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
[[maybe_unused]] void TestBatchWriteThenReadMultiTabletInterleaved(NDDisk::TDDiskConfig ddiskConfig,
        ui32 writeSeed, NLog::EPriority ddiskLogPriority = NLog::PRI_INFO) {
    constexpr ui32 numTablets = 10;
    constexpr ui32 numVChunks = 5;
    constexpr ui32 blocksPerVChunk = 8;
    constexpr ui32 totalWrites = numTablets * numVChunks * blocksPerVChunk;
    constexpr ui32 baseTabletId = 301;

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

    for (const auto& op : writeOps) {
        ui32 blockIdx = op.vchunkIdx * blocksPerVChunk + op.blockInChunk;
        TString payload = MakeDataWithTabletAndBlock(baseTabletId + op.tabletIdx, blockIdx, MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds[op.tabletIdx],
            NDDisk::TBlockSelector(op.vchunkIdx, op.blockInChunk * MinBlockSize, MinBlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));
        ctx.Send(write.release());
    }

    for (ui32 i = 0; i < totalWrites; ++i) {
        auto writeResult = ctx.Grab<NDDisk::TEvWriteResult>();
        AssertStatus<NDDisk::TEvWriteResult>(writeResult, TReplyStatus::OK);
    }

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
                const ui32* readPtr = reinterpret_cast<const ui32*>(actual.data());
                UNIT_ASSERT_VALUES_EQUAL_C(readPtr[0], tabletId,
                    "tablet id mismatch at tablet=" << tabletId << " vchunk=" << v << " block=" << b);
                UNIT_ASSERT_VALUES_EQUAL(actual, expected);
            }
        }
    }
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

} // anonymous namespace
