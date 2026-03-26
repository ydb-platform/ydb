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

namespace NKikimr {
namespace {

using NKikimrBlobStorage::NDDisk::TReplyStatus;

constexpr ui32 NodeId = 1;
constexpr ui32 MinBlockSize = 4096;
constexpr ui32 PDiskId = 1;
constexpr ui32 SlotId = 1;
constexpr ui32 ChunkSize = 16u << 20;
constexpr ui64 DefaultPDiskSequence = 0x7e5700007e570000;

void FormatDisk(const TString& path, ui64 guid, ui32 chunkSize) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    EntropyPool().Read(&chunkKey, sizeof(chunkKey));
    EntropyPool().Read(&logKey, sizeof(logKey));
    EntropyPool().Read(&sysLogKey, sizeof(sysLogKey));

    TFormatOptions options;
    options.EnableSmallDiskOptimization = true;
    FormatPDisk(path, (ui64)chunkSize * 1000, 4 << 10, chunkSize, guid,
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

    explicit TTestContext(NDDisk::TDDiskConfig ddiskConfig = {}) {
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        Runtime.Reset(new NActors::TTestActorRuntime(1, 1, true));

        auto appData = MakeHolder<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        appData->IoContextFactory = IoContext.get();

        Runtime->SetLogBackend(CreateStderrBackend());
        Runtime->Initialize(NActors::TTestActorRuntime::TEgg{appData.Release(), nullptr, {}, {}, {}});
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_ERROR);
        Runtime->SetLogPriority(NKikimrServices::BS_DDISK, NLog::PRI_DEBUG);

        Edge = Runtime->AllocateEdgeActor();

        TString path = TempDir() + "/pdisk.dat";
        {
            TFile file(path.c_str(), OpenAlways | RdWr);
            file.Resize((ui64)ChunkSize * 1000);
            file.Close();
        }

        const ui64 pdiskGuid = 12345;
        FormatDisk(path, pdiskGuid, ChunkSize);

        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig(path, pdiskGuid, PDiskId, 0);
        pdiskConfig->ChunkSize = ChunkSize;
        pdiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pdiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(true);

        NPDisk::TMainKey mainKey{.Keys = {DefaultPDiskSequence}, .IsInitialized = true};
        IActor* pdiskActor = CreatePDisk(pdiskConfig.Get(), mainKey, Counters);
        TActorId pdiskActorId = Runtime->Register(pdiskActor);
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, PDiskId);
        Runtime->RegisterService(pdiskServiceId, pdiskActorId);

        TVector<TActorId> actorIds = {
            MakeBlobStorageDDiskId(NodeId, PDiskId, SlotId),
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

        // TODO: remove when separated
        NDDisk::TPersistentBufferFormat pbFormat{256, 4, ChunkSize, 8};
        IActor* ddiskActor = NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), std::move(ddiskConfig), Counters);

        TActorId ddiskActorId = Runtime->Register(ddiskActor);
        DDiskServiceId = MakeBlobStorageDDiskId(NodeId, PDiskId, SlotId);
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
    memcpy(buf.GetDataMut(), data.data(), data.size());
    return TRope(std::move(buf));
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

void TestWriteAndRead(NDDisk::TDDiskConfig ddiskConfig, ui32 blockSize) {
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

void TestCheckVChunksArePerTablet(NDDisk::TDDiskConfig ddiskConfig) {
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

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDDiskActorPDiskTest) {
    Y_UNIT_TEST(WriteAndRead_4KiB_Uring) {
        TestWriteAndRead({}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_4KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_Uring) {
        TestWriteAndRead({}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_Uring) {
        TestWriteAndRead({}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 1_MB);
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_Uring) {
        TestCheckVChunksArePerTablet({});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_PDiskFallback) {
        TestCheckVChunksArePerTablet({.ForcePDiskFallback = true});
    }
}

} // NKikimr
