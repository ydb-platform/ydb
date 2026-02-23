#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <algorithm>
#include <cstring>

namespace NKikimr {
namespace {

using NKikimrBlobStorage::NDDisk::TReplyStatus;

constexpr ui32 NodeId = 1;
constexpr ui32 BlockSize = 4096;

static_assert(NDDisk::NPrivate::THasSelectorField<NKikimrBlobStorage::NDDisk::TEvWrite>::value);

struct TDiskHandle {
    TActorId ServiceId;
    TActorId PDiskEdge;
    ui32 PDiskId;
    ui32 SlotId;
};

class TTestContext {
    template<typename TEvent>
    static std::unique_ptr<TEventHandle<TEvent>> RecastEvent(std::unique_ptr<IEventHandle> ev) {
        return std::unique_ptr<TEventHandle<TEvent>>(reinterpret_cast<TEventHandle<TEvent>*>(ev.release()));
    }

    static void SendFromPDisk(TTestActorSystem& runtime, const TActorId& sender, const TActorId& recipient,
            IEventBase* ev, ui64 cookie = 0) {
        runtime.Send(new IEventHandle(recipient, sender, ev, 0, cookie), NodeId);
    }

public:
    TTestActorSystem Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorId Edge;

    TTestContext()
        : Runtime(1)
        , Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
    {
        Runtime.Start();
        Edge = Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
    }

    ~TTestContext() {
        Runtime.Stop();
    }

    TDiskHandle CreateDDisk(ui32 pdiskId, ui32 slotId) {
        const TActorId pdiskEdge = Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        const TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);
        Runtime.RegisterService(pdiskServiceId, pdiskEdge);

        TVector<TActorId> actorIds = {
            MakeBlobStorageDDiskId(NodeId, pdiskId, slotId),
        };
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone, ui32(1), ui32(1),
            ui32(1), &actorIds);

        TVDiskConfig::TBaseInfo baseInfo(
            TVDiskIdShort(groupInfo->GetVDiskId(0)),
            pdiskServiceId,
            0x100000 + pdiskId,
            pdiskId,
            NPDisk::DEVICE_TYPE_NVME,
            slotId,
            NKikimrBlobStorage::TVDiskKind::Default,
            1,
            "ddisk_pool");

        const TActorId ddiskActor = Runtime.Register(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo, Counters),
            NodeId);
        const TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, slotId);
        Runtime.RegisterService(ddiskServiceId, ddiskActor);

        TDiskHandle disk{ddiskServiceId, pdiskEdge, pdiskId, slotId};
        BootstrapDDisk(disk);

        return disk;
    }

    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequest(const TDiskHandle& disk) {
        std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
        UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TEvent::EventType);
        return RecastEvent<TEvent>(std::move(raw));
    }

    template<typename TRequestEvent>
    void SendPDiskResponse(const TDiskHandle& disk, const TEventHandle<TRequestEvent>& request, IEventBase* response) {
        SendFromPDisk(Runtime, disk.PDiskEdge, request.Sender, response, request.Cookie);
    }

    void BootstrapDDisk(const TDiskHandle& disk) {
        constexpr ui32 ChunkSize = 128u << 20;
        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;
        constexpr ui32 MinChunksReserved = 2;
        constexpr ui32 PersistentBufferInitChunks = 4;

        auto init = WaitPDiskRequest<NPDisk::TEvYardInit>(disk);
        TVector<ui32> ownedChunks;
        auto initReply = std::make_unique<NPDisk::TEvYardInitResult>(
            NKikimrProto::OK,
            0, 0, 0, // seek/read/write speed
            BlockSize, BlockSize, BlockSize,
            ChunkSize,
            BlockSize,
            Owner,
            OwnerRound,
            1, // slot size in units
            0, // status flags
            std::move(ownedChunks),
            NPDisk::DEVICE_TYPE_NVME,
            false,
            BlockSize,
            "");

        NPDisk::TDiskFormat format;
        format.Clear(false);
        initReply->DiskFormat = NPDisk::TDiskFormatPtr(new NPDisk::TDiskFormat(format), +[](NPDisk::TDiskFormat* ptr) {
            delete ptr;
        });
        SendPDiskResponse(disk, *init, initReply.release());

        auto readLog = WaitPDiskRequest<NPDisk::TEvReadLog>(disk);
        auto readLogReply = std::make_unique<NPDisk::TEvReadLogResult>(
            NKikimrProto::OK,
            readLog->Get()->Position,
            readLog->Get()->Position,
            true, // end of log
            0,    // status flags
            "",
            Owner);
        SendPDiskResponse(disk, *readLog, readLogReply.release());

        // DDisk bootstrap starts persistent buffer initialization in background.
        // Burn these PDisk requests here, so later client-only phases don't see unsolicited PDisk traffic.
        auto reserve = WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, MinChunksReserved);
        auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        const ui32 startupReserveChunks = PersistentBufferInitChunks + MinChunksReserved;
        for (ui32 i = 0; i < startupReserveChunks; ++i) {
            reserveReply->ChunkIds.push_back(100000 + disk.PDiskId * 1000 + i);
        }
        SendPDiskResponse(disk, *reserve, reserveReply.release());

        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            auto log = WaitPDiskRequest<NPDisk::TEvLog>(disk);
            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            SendPDiskResponse(disk, *log, logReply.release());
        }
    }
};

void SendToDDisk(TTestContext& ctx, const TActorId& serviceId, IEventBase* event, ui64 cookie = 0) {
    ctx.Runtime.Send(new IEventHandle(serviceId, ctx.Edge, event, 0, cookie), NodeId);
}

template<typename TResponseEvent>
std::unique_ptr<TEventHandle<TResponseEvent>> WaitFromDDisk(TTestContext& ctx) {
    return ctx.Runtime.WaitForEdgeActorEvent<TResponseEvent>(ctx.Edge, false);
}

template<typename TResponseEvent>
std::unique_ptr<TEventHandle<TResponseEvent>> SendToDDiskAndWait(TTestContext& ctx, const TActorId& serviceId,
        IEventBase* event, ui64 cookie = 0) {
    SendToDDisk(ctx, serviceId, event, cookie);
    return WaitFromDDisk<TResponseEvent>(ctx);
}

template<typename TResponseEvent>
void AssertStatus(const std::unique_ptr<TEventHandle<TResponseEvent>>& ev, TReplyStatus::E status) {
    const auto actual = static_cast<TReplyStatus::E>(ev->Get()->Record.GetStatus());
    UNIT_ASSERT_C(actual == status, TStringBuilder()
        << "actual# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(actual)
        << " expected# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(status));
}

TString MakeData(char ch, ui32 size) {
    TString data = TString::Uninitialized(size);
    memset(data.Detach(), ch, data.size());
    return data;
}

NDDisk::TQueryCredentials Connect(TTestContext& ctx, const TActorId& serviceId, ui64 tabletId, ui32 generation) {
    NDDisk::TQueryCredentials creds;
    creds.TabletId = tabletId;
    creds.Generation = generation;

    auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, serviceId, new NDDisk::TEvConnect(creds));
    AssertStatus(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

    return creds;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDDiskActorTest) {
    Y_UNIT_TEST(SessionValidation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(1, 1);

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 1;
        creds.Generation = 1;

        auto noSessionRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(noSessionRead, TReplyStatus::SESSION_MISMATCH);

        auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(creds));
        AssertStatus(connectResult, TReplyStatus::OK);
        creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

        NDDisk::TQueryCredentials badGuid = creds;
        badGuid.DDiskInstanceGuid = *badGuid.DDiskInstanceGuid + 1;
        auto wrongGuidRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(badGuid, {0, 0, BlockSize}, {true}));
        AssertStatus(wrongGuidRead, TReplyStatus::SESSION_MISMATCH);

        auto disconnect = std::make_unique<NDDisk::TEvDisconnect>();
        creds.Serialize(disconnect->Record.MutableCredentials());
        auto disconnectResult = SendToDDiskAndWait<NDDisk::TEvDisconnectResult>(ctx, disk.ServiceId,
            disconnect.release());
        AssertStatus(disconnectResult, TReplyStatus::OK);

        auto readAfterDisconnect = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(readAfterDisconnect, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(ConnectGenerationRules) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(2, 1);

        NDDisk::TQueryCredentials gen2;
        gen2.TabletId = 11;
        gen2.Generation = 2;
        auto gen2Connect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(gen2));
        AssertStatus(gen2Connect, TReplyStatus::OK);
        gen2.DDiskInstanceGuid = gen2Connect->Get()->Record.GetDDiskInstanceGuid();

        NDDisk::TQueryCredentials gen1 = gen2;
        gen1.Generation = 1;
        auto obsoleteConnect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(gen1));
        AssertStatus(obsoleteConnect, TReplyStatus::BLOCKED);

        NDDisk::TQueryCredentials gen3 = gen2;
        gen3.Generation = 3;
        auto gen3Connect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(gen3));
        AssertStatus(gen3Connect, TReplyStatus::OK);
        gen3.DDiskInstanceGuid = gen3Connect->Get()->Record.GetDDiskInstanceGuid();

        auto queryWithLatestGeneration = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(gen3, {0, 0, BlockSize}, {true}));
        AssertStatus(queryWithLatestGeneration, TReplyStatus::OK);

        auto queryWithOldGeneration = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(gen2, {0, 0, BlockSize}, {true}));
        AssertStatus(queryWithOldGeneration, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(IncorrectRequestValidation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(3, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 10, 1);

        auto misaligned = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 1, BlockSize),
            NDDisk::TWriteInstruction(0));
        misaligned->AddPayload(TRope(MakeData('A', BlockSize)));
        auto misalignedResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, misaligned.release());
        AssertStatus(misalignedResult, TReplyStatus::INCORRECT_REQUEST);

        auto wrongSize = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 0, BlockSize),
            NDDisk::TWriteInstruction(0));
        wrongSize->AddPayload(TRope(MakeData('B', 2 * BlockSize)));
        auto wrongSizeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, wrongSize.release());
        AssertStatus(wrongSizeResult, TReplyStatus::INCORRECT_REQUEST);

        auto zeroSizeRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, 0}, {true}));
        AssertStatus(zeroSizeRead, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(ReadFromUnallocatedChunkReturnsZeroes) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {42, 0, 2 * BlockSize}, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT(readResult->Get()->Record.HasReadResult());
        UNIT_ASSERT(readResult->Get()->Record.GetReadResult().HasPayloadId());

        const TString data = readResult->Get()->GetPayload(0).ConvertToString();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), 2 * BlockSize);
        UNIT_ASSERT(std::all_of(data.begin(), data.end(), [](char c) { return c == '\0'; }));
    }

    Y_UNIT_TEST(WriteThenReadRoundtrip) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 30, 1);

        const TString payload = MakeData('Q', 2 * BlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, BlockSize, static_cast<ui32>(payload.size())), NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));

        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto logIncrement = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logIncrementReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logIncrementReply->Results.emplace_back(logIncrement->Get()->Lsn, logIncrement->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logIncrement, logIncrementReply.release());

        auto logSnapshot = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logSnapshotReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logSnapshotReply->Results.emplace_back(logSnapshot->Get()->Lsn, logSnapshot->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logSnapshot, logSnapshotReply.release());

        auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        UNIT_ASSERT_VALUES_EQUAL(refill->Get()->SizeChunks, 1u);
        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(1003);
        ctx.SendPDiskResponse(disk, *refill, refillReply.release());

        auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        const ui32 allocatedChunk = writeRaw->Get()->ChunkIdx;
        UNIT_ASSERT(allocatedChunk != 0u);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Offset, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds,
            {7, BlockSize, static_cast<ui32>(payload.size())}, {true}));

        auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->ChunkIdx, allocatedChunk);
        UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Offset, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Size, payload.size());
        ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(payload)));

        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
    }

    Y_UNIT_TEST(PersistentBufferLifecycle) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.ServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.ServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 1);
        const auto& record = listResult->Get()->Record.GetRecords(0);
        UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetVChunkIndex(), selector.VChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetOffsetInBytes(), selector.OffsetInBytes);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetSize(), selector.Size);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvErasePersistentBuffer(creds, selector, lsn));

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        auto missingRead = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.ServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, {true}));
        AssertStatus(missingRead, TReplyStatus::MISSING_RECORD);

    }
}

} // NKikimr
