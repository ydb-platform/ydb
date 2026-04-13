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
constexpr ui32 MinChunksReserved = 2;
constexpr ui32 PersistentBufferInitChunks = 4;

static_assert(NDDisk::NPrivate::THasSelectorField<NKikimrBlobStorage::NDDisk::TEvWrite>::value);

struct TDiskHandle {
    TActorId ServiceId;
    TActorId PBServiceId;
    TActorId PDiskEdge;
    ui32 PDiskId;
    ui32 SlotId;
    ui32 FirstChunkId;
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
    static constexpr ui32 ChunkSize = 128u << 20;

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
        NDDisk::TPersistentBufferFormat pbFormat{256, 4, 128 << 20, 8};
        const TActorId ddiskActor = Runtime.Register(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), NDDisk::TDDiskConfig{}, Counters),
            NodeId);
        const TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, slotId);
        const TActorId pbServiceId = MakeBlobStoragePersistentBufferId(NodeId, pdiskId, slotId);
        Runtime.RegisterService(ddiskServiceId, ddiskActor);

        TDiskHandle disk{ddiskServiceId, pbServiceId, pdiskEdge, pdiskId, slotId, 100000 + pdiskId * 1000};
        BootstrapDDisk(disk);

        return disk;
    }

    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequest(const TDiskHandle& disk) {
        std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
        UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TEvent::EventType);
        return RecastEvent<TEvent>(std::move(raw));
    }

    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequests(const std::set<TActorId>& disks) {
        std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent(disks);
        UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TEvent::EventType);
        return RecastEvent<TEvent>(std::move(raw));
    }

    template<typename TRequestEvent>
    void SendPDiskResponse(const TDiskHandle& disk, const TEventHandle<TRequestEvent>& request, IEventBase* response) {
        SendFromPDisk(Runtime, disk.PDiskEdge, request.Sender, response, request.Cookie);
    }

    void BootstrapDDisk(const TDiskHandle& disk) {
        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;

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

        NPDisk::TDiskFormat format = {};
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
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + i);
        }
        SendPDiskResponse(disk, *reserve, reserveReply.release());

        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            auto log = WaitPDiskRequest<NPDisk::TEvLog>(disk);
            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            SendPDiskResponse(disk, *log, logReply.release());
        }
        auto checkSpace = WaitPDiskRequest<NPDisk::TEvCheckSpace>(disk);
        auto res = new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0);
        SendPDiskResponse(disk, *checkSpace, res);
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

TRope MakeAlignedRope(const TString& data) {
    auto buf = TRcBuf::UninitializedPageAligned(data.size());
    memcpy(buf.GetDataMut(), data.data(), data.size());
    return TRope(std::move(buf));
}

TRope MakeMisalignedRope(const TString& data) {
    auto buf = TRcBuf::UninitializedPageAligned(data.size() + BlockSize);
    memcpy(buf.GetDataMut() + 1, data.data(), data.size());
    return TRope(TRcBuf(TRcBuf::Piece, buf.data() + 1, data.size(), buf));
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

struct TInitialWriteOutcome {
    std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>> WriteResult;
    ui32 ChunkIdx = 0;
};

/** First write to a vchunk: PDisk sees log records, chunk reserve, then TEvChunkWriteRaw. */
TInitialWriteOutcome DoWriteWithChunkAllocation(TTestContext& ctx, const TDiskHandle& disk, std::unique_ptr<NDDisk::TEvWrite> write,
        ui32 chunkId, ui32 expectedOffsetInBytes, const TString& expectedPayload,
        bool reserveExpected, bool checkSnapshot) {
    SendToDDisk(ctx, disk.ServiceId, write.release());

    if (!reserveExpected) {
        // no existing reserve: have to request chunk
        auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        UNIT_ASSERT_VALUES_EQUAL(refill->Get()->SizeChunks, 2u);

        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(chunkId);
        refillReply->ChunkIds.push_back(chunkId + 1);
        ctx.SendPDiskResponse(disk, *refill, refillReply.release());
    }

    auto logIncrement = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
    auto logIncrementReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
    logIncrementReply->Results.emplace_back(logIncrement->Get()->Lsn, logIncrement->Get()->Cookie);
    ctx.SendPDiskResponse(disk, *logIncrement, logIncrementReply.release());

    if (checkSnapshot) {
        auto logSnapshot = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logSnapshotReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logSnapshotReply->Results.emplace_back(logSnapshot->Get()->Lsn, logSnapshot->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logSnapshot, logSnapshotReply.release());
    }

    // DDisk took 1 chunk from reserve, it will request one to have reserve
    auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
    UNIT_ASSERT_VALUES_EQUAL(refill->Get()->SizeChunks, 1u);
    auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
    refillReply->ChunkIds.push_back(chunkId + 2);
    ctx.SendPDiskResponse(disk, *refill, refillReply.release());

    auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
    const ui32 chunkIdx = writeRaw->Get()->ChunkIdx;
    UNIT_ASSERT(chunkIdx != 0u);
    UNIT_ASSERT_VALUES_EQUAL(chunkIdx, chunkId);
    UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Offset, expectedOffsetInBytes);
    UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Data.ConvertToString(), expectedPayload);
    ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

    return TInitialWriteOutcome{WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), chunkIdx};
}

/** Subsequent writes to an already allocated chunk: only TEvChunkWriteRaw, then TEvWriteResult. */
std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>> DoWrite(TTestContext& ctx, const TDiskHandle& disk,
        std::unique_ptr<NDDisk::TEvWrite> write) {
    SendToDDisk(ctx, disk.ServiceId, write.release());
    auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
    ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
    return WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
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

        ui32 offset = 0;

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {42, offset, 2 * BlockSize}, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT(readResult->Get()->Record.HasReadResult());
        UNIT_ASSERT(readResult->Get()->Record.GetReadResult().HasPayloadId());

        const TString data = readResult->Get()->GetPayload(0).ConvertToString();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), 2 * BlockSize);
        UNIT_ASSERT(std::all_of(data.begin(), data.end(), [](char c) { return c == '\0'; }));
    }

    Y_UNIT_TEST(NoZeroRead) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        ui32 offset = 0;

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {42, offset, 0}, {true}));
        AssertStatus(readResult, TReplyStatus::INCORRECT_REQUEST);
        UNIT_ASSERT(!readResult->Get()->Record.HasReadResult());
    }

    Y_UNIT_TEST(ReadOffsetShouldBeBlockAligned) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        for (ui32 offset: {1U, 2U, BlockSize - 1}) {
            auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
                ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {42, offset, BlockSize}, {true}));
            AssertStatus(readResult, TReplyStatus::INCORRECT_REQUEST);
            UNIT_ASSERT(!readResult->Get()->Record.HasReadResult());
        }
    }

    Y_UNIT_TEST(ReadShouldBeWithinChunk) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        ui32 offset = ctx.ChunkSize - BlockSize;

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {42, offset, 2 * BlockSize}, {true}));
        AssertStatus(readResult, TReplyStatus::INCORRECT_REQUEST);
        UNIT_ASSERT(!readResult->Get()->Record.HasReadResult());
    }

    Y_UNIT_TEST(WriteOffsetShouldBeBlockAligned) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        for (ui32 offset: {1U, 2U, BlockSize - 1}) {
            auto write = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(42, offset, BlockSize),
                NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(MakeData('W', BlockSize)));
            auto writeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, write.release());
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
    }

    Y_UNIT_TEST(WriteShouldBeWithinChunk) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        ui32 offset = ctx.ChunkSize - BlockSize;

        auto write = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(42, offset, 2 * BlockSize),
            NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(MakeData('W', 2 * BlockSize)));
        auto writeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, write.release());
        AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(WritePayloadMustBeContiguousAndAligned) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(4, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 20, 1);

        {
            auto write = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 0, BlockSize),
                NDDisk::TWriteInstruction(0));
            write->AddPayload(MakeMisalignedRope(MakeData('U', BlockSize)));
            auto writeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, write.release());
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }

        {
            TString part1 = MakeData('X', BlockSize / 2);
            TString part2 = MakeData('Y', BlockSize / 2);
            TRope nonContiguous;
            nonContiguous.Insert(nonContiguous.End(), TRope(part1));
            nonContiguous.Insert(nonContiguous.End(), TRope(part2));
            UNIT_ASSERT_VALUES_EQUAL(nonContiguous.size(), BlockSize);

            auto write = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 0, BlockSize),
                NDDisk::TWriteInstruction(0));
            write->AddPayload(std::move(nonContiguous));
            auto writeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, write.release());
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
    }

    Y_UNIT_TEST(WriteAndRead) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 30, 1);

        // initial write-read

        const TString payload = MakeData('Q', 2 * BlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, BlockSize, static_cast<ui32>(payload.size())), NDDisk::TWriteInstruction(0));
        write->AddPayload(MakeAlignedRope(payload));

        auto initial = DoWriteWithChunkAllocation(
            ctx, disk, std::move(write), disk.FirstChunkId + PersistentBufferInitChunks, BlockSize, payload, true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);
        const ui32 allocatedChunk = initial.ChunkIdx;

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

        // Second write to the same vchunk: only TEvChunkWriteRaw (DoWriteWithChunkAllocation's log/reserve path must not run)

        const TString payload2 = MakeData('R', 2 * BlockSize);
        const ui32 secondOffset = BlockSize + static_cast<ui32>(payload.size());
        auto write2 = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, secondOffset, static_cast<ui32>(payload2.size())), NDDisk::TWriteInstruction(0));
        write2->AddPayload(MakeAlignedRope(payload2));
        auto secondWriteResult = DoWrite(ctx, disk, std::move(write2));
        AssertStatus(secondWriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds,
            {7, secondOffset, static_cast<ui32>(payload2.size())}, {true}));

        auto readRaw2 = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(readRaw2->Get()->ChunkIdx, allocatedChunk);
        UNIT_ASSERT_VALUES_EQUAL(readRaw2->Get()->Offset, secondOffset);
        UNIT_ASSERT_VALUES_EQUAL(readRaw2->Get()->Size, payload2.size());
        ctx.SendPDiskResponse(disk, *readRaw2, new NPDisk::TEvChunkReadRawResult(TRope(payload2)));

        auto readResult2 = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult2, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Get()->GetPayload(0).ConvertToString(), payload2);
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(9, 1);

        auto blockPayload = [](const char* lit, size_t litLen) {
            UNIT_ASSERT_C(litLen <= BlockSize, "literal too long for block");
            TString s(BlockSize, '\0');
            memcpy(s.begin(), lit, litLen);
            return s;
        };
        const TString payload1 = blockPayload("tablet1", 7);
        const TString payload2 = blockPayload("tablet2", 7);

        const ui32 chunkTablet1 = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 chunkTablet2 = disk.FirstChunkId + PersistentBufferInitChunks + 1;

        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk.ServiceId, 101, 1);
        {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds1, NDDisk::TBlockSelector(0, 0, BlockSize),
                NDDisk::TWriteInstruction(0));
            w->AddPayload(MakeAlignedRope(payload1));
            auto initial = DoWriteWithChunkAllocation(ctx, disk, std::move(w), chunkTablet1, 0, payload1, true, true);
            AssertStatus(initial.WriteResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(initial.ChunkIdx, chunkTablet1);
        }

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds1, {0, 0, BlockSize}, {true}));
        {
            auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->ChunkIdx, chunkTablet1);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Offset, 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Size, BlockSize);
            ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(payload1)));
        }
        auto read1 = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(read1, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(read1->Get()->GetPayload(0).ConvertToString(), payload1);

        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk.ServiceId, 102, 1);

        auto expectUnallocatedZeroes = [&](ui64 vChunk) {
            auto rr = SendToDDiskAndWait<NDDisk::TEvReadResult>(
                ctx, disk.ServiceId, new NDDisk::TEvRead(creds2, {vChunk, 0, BlockSize}, {true}));
            AssertStatus(rr, TReplyStatus::OK);
            const TString data = rr->Get()->GetPayload(0).ConvertToString();
            UNIT_ASSERT_VALUES_EQUAL(data.size(), BlockSize);
            UNIT_ASSERT(std::all_of(data.begin(), data.end(), [](char c) { return c == '\0'; }));
        };
        expectUnallocatedZeroes(0);
        expectUnallocatedZeroes(2);

        {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds2, NDDisk::TBlockSelector(0, 0, BlockSize),
                NDDisk::TWriteInstruction(0));
            w->AddPayload(MakeAlignedRope(payload2));
            auto initial = DoWriteWithChunkAllocation(ctx, disk, std::move(w), chunkTablet2, 0, payload2, true, false);
            AssertStatus(initial.WriteResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(initial.ChunkIdx, chunkTablet2);
        }

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds2, {0, 0, BlockSize}, {true}));
        {
            auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->ChunkIdx, chunkTablet2);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Offset, 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Size, BlockSize);
            ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(payload2)));
        }
        auto read2 = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(read2, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(read2->Get()->GetPayload(0).ConvertToString(), payload2);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds1, {0, 0, BlockSize}, {true}));
        {
            auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->ChunkIdx, chunkTablet1);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Offset, 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->Size, BlockSize);
            ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(payload1)));
        }
        auto read1Again = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(read1Again, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(read1Again->Get()->GetPayload(0).ConvertToString(), payload1);
    }

    Y_UNIT_TEST(PersistentBufferLifecycle) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 1);
        const auto& record = listResult->Get()->Record.GetRecords(0);
        UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetVChunkIndex(), selector.VChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetOffsetInBytes(), selector.OffsetInBytes);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetSize(), selector.Size);

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn, 1));

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        auto missingRead = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
        AssertStatus(missingRead, TReplyStatus::MISSING_RECORD);

    }

    Y_UNIT_TEST(PersistentBufferWriteTunnel) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);
        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{{NodeId, disk1.PDiskId, disk1.SlotId}, {NodeId, disk2.PDiskId, disk2.SlotId}, {NodeId, disk3.PDiskId, disk3.SlotId}};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn, NDDisk::TWriteInstruction(0)
            , pbs, 1000);
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk1.PBServiceId, write.release());
        for (auto disk : {disk1, disk2, disk3}) {
            auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }

        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
            ctx.Edge, false);
        UNIT_ASSERT(writeResult->Get()->Record.ResultSize() == 3);
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); i++) {
            auto& wr = writeResult->Get()->Record.GetResult(i);
            UNIT_ASSERT(wr.GetResult().GetStatus() == TReplyStatus::OK);

        }
        for (auto disk : {disk1, disk2, disk3}) {
            creds = Connect(ctx, disk.PBServiceId, 40, 1);
            auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

            auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
            AssertStatus(listResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 1);
            const auto& record = listResult->Get()->Record.GetRecords(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetVChunkIndex(), selector.VChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetOffsetInBytes(), selector.OffsetInBytes);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetSize(), selector.Size);
        }
    }

    Y_UNIT_TEST(PersistentBufferWriteTunnel_DelayedResponse) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);
        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{{NodeId, disk1.PDiskId, disk1.SlotId}, {NodeId, disk2.PDiskId, disk2.SlotId}, {NodeId, disk3.PDiskId, disk3.SlotId}};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn, NDDisk::TWriteInstruction(0)
            , pbs, 1000);
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk1.PBServiceId, write.release());
        for (auto disk : {disk1, disk2}) {
            auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }
        auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        // Simulate disk3 response was not received in 1000 microseconds
        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
            ctx.Edge, false);
        UNIT_ASSERT(writeResult->Get()->Record.ResultSize() == 2);
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); i++) {
            auto& wr = writeResult->Get()->Record.GetResult(i);
            UNIT_ASSERT(wr.GetResult().GetStatus() == TReplyStatus::OK);
        }
        ctx.SendPDiskResponse(disk1, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        // Waiting disk3 results
        writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
            ctx.Edge, false);
        UNIT_ASSERT(writeResult->Get()->Record.ResultSize() == 1);
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); i++) {
            auto& wr = writeResult->Get()->Record.GetResult(i);
            UNIT_ASSERT(wr.GetResult().GetStatus() == TReplyStatus::OK);
        }
    }

    void DoTest(const std::vector<TReplyStatus::E> expected) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);
        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{{NodeId, disk1.PDiskId, disk1.SlotId}, {NodeId, disk2.PDiskId, disk2.SlotId}, {NodeId, disk3.PDiskId, disk3.SlotId}};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn, NDDisk::TWriteInstruction(0)
            , pbs, 1000);
        write->AddPayload(TRope(payload));
        ui32 okCnt = 0;

        ctx.Runtime.FilterFunction = [&](ui32 _, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffer::EventType) {
                // first cookie is for TEvWritePersistentBuffers, so we do decrement
                return expected[ev->Cookie - 1] != TReplyStatus::ERROR;
            }
            if (ev->GetTypeRewrite() == NDDisk::TEvWritePersistentBufferResult::EventType) {
                okCnt--;
                if (okCnt == 0) {
                    ctx.Runtime.Send(new IEventHandle(ev->Recipient, ev->Sender,
                        new TEvInterconnect::TEvNodeDisconnected(1), 0, 0), 1);

                }
            }
            return true;
        };

        SendToDDisk(ctx, disk1.PBServiceId, write.release());
        for (auto s : expected) {
            if (s == TReplyStatus::OK) {
                okCnt++;
                auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
                ctx.SendPDiskResponse(disk1, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            }
        }

        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
            ctx.Edge, false);
        UNIT_ASSERT(writeResult->Get()->Record.ResultSize() == 3);
        UNIT_ASSERT(okCnt == 0);
        for (auto s : expected) {
            if (s == TReplyStatus::OK) {
                okCnt++;
            }
        }
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); i++) {
            auto& wr = writeResult->Get()->Record.GetResult(i);
            if (wr.GetResult().GetStatus() == TReplyStatus::OK) {
                okCnt--;
            }
        }
        UNIT_ASSERT(okCnt == 0);
    }

    Y_UNIT_TEST(PersistentBufferWriteTunnel_Mixed1) {
        DoTest({TReplyStatus::OK, TReplyStatus::OK, TReplyStatus::ERROR});
    }

    Y_UNIT_TEST(PersistentBufferWriteTunnel_Mixed2) {
        DoTest({TReplyStatus::ERROR, TReplyStatus::OK, TReplyStatus::ERROR});
    }

    Y_UNIT_TEST(PersistentBufferPDiskOccupancy) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};
        auto checkSpace = ctx.WaitPDiskRequest<NPDisk::TEvCheckSpace>(disk);
        auto res = new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0);
        double expected = 0.123;
        res->NormalizedOccupancy = expected;
        ctx.SendPDiskResponse(disk, *checkSpace, res);

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
        UNIT_ASSERT(writeResult->Get()->Record.GetPDiskNormalizedOccupancy() == expected);

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn, 1));

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);
        UNIT_ASSERT(eraseResult->Get()->Record.GetPDiskNormalizedOccupancy() == expected);
    }

    Y_UNIT_TEST(PersistentBufferTabletGeneration) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);
        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk.PBServiceId, 40, 2);
        auto write2 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds2, selector, lsn, NDDisk::TWriteInstruction(0));
        const TString payload2 = MakeData('Q', BlockSize);
        write2->AddPayload(TRope(payload2));
        SendToDDisk(ctx, disk.PBServiceId, write2.release());

        pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto write2Result = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(write2Result, TReplyStatus::OK);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2));
        AssertStatus(listResult, TReplyStatus::OK);
        ui32 gen1Count = 0;
        ui32 gen2Count = 0;
        UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 2);
        for (ui32 i : xrange(2)) {
            const auto& record = listResult->Get()->Record.GetRecords(i);
            UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetVChunkIndex(), selector.VChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetOffsetInBytes(), selector.OffsetInBytes);

            const ui32 generation = record.GetGeneration();
            UNIT_ASSERT(generation == 1 || generation == 2);
            if (generation == 1) {
                ++gen1Count;
            } else if (generation == 2) {
                ++gen2Count;
            }
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetSize(), selector.Size);
        }
        UNIT_ASSERT_VALUES_EQUAL(gen1Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(gen2Count, 1);
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selector, lsn, 2, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload2);

        readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
    }

    Y_UNIT_TEST(PersistentBufferReadPart) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const ui32 size = BlockSize * 10;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        const NDDisk::TBlockSelector readSelector{3, BlockSize * 3, BlockSize * 5};

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, readSelector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload.substr(BlockSize * 3, BlockSize * 5));
    }

    Y_UNIT_TEST(PersistentBufferReadThenWriteTunnel) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);
        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};
        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{{NodeId, disk2.PDiskId, disk2.SlotId}, {NodeId, disk3.PDiskId, disk3.SlotId}};

        {
            auto write1 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write1->AddPayload(TRope(payload));
            SendToDDisk(ctx, disk1.PBServiceId, write1.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk1, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult1 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult1, TReplyStatus::OK);
        }

        {
            // Request for lsn does not exist
            auto write1 = std::make_unique<NDDisk::TEvReadThenWritePersistentBuffers>(creds, 123, 1, pbs, 1000);
            SendToDDisk(ctx, disk1.PBServiceId, write1.release());
            auto writeResult1 = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
                ctx.Edge, false);
            UNIT_ASSERT(writeResult1->Get()->Record.ResultSize() == 2);
            for (ui32 i = 0; i < writeResult1->Get()->Record.ResultSize(); i++) {
                auto& wr = writeResult1->Get()->Record.GetResult(i);
                UNIT_ASSERT(wr.GetResult().GetStatus() == TReplyStatus::MISSING_RECORD);
            }
        }

        auto write = std::make_unique<NDDisk::TEvReadThenWritePersistentBuffers>(creds, lsn, 1, pbs, 1000);
        SendToDDisk(ctx, disk1.PBServiceId, write.release());
        for (auto disk : {disk2, disk3}) {
            auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk2.PDiskEdge, disk3.PDiskEdge});
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }

        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(
            ctx.Edge, false);
        UNIT_ASSERT(writeResult->Get()->Record.ResultSize() == 2);
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); i++) {
            auto& wr = writeResult->Get()->Record.GetResult(i);
            UNIT_ASSERT(wr.GetResult().GetStatus() == TReplyStatus::OK);

        }
        for (auto disk : {disk1, disk2, disk3}) {
            creds = Connect(ctx, disk.PBServiceId, 40, 1);
            auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

            auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
            AssertStatus(listResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 1);
            const auto& record = listResult->Get()->Record.GetRecords(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetVChunkIndex(), selector.VChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetOffsetInBytes(), selector.OffsetInBytes);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSelector().GetSize(), selector.Size);
        }
    }

    Y_UNIT_TEST(SyncFailWhenRequestToSourceIsUndelivered) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(10, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 99;
        const ui32 srcSlotId = 1;
        TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId = MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId);
        ctx.Runtime.RegisterService(fakeSourceServiceId, fakeSourceEdge);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(NodeId, srcPDiskId, srcSlotId),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new TEvents::TEvUndelivered(NDDisk::TEv::EvRead, TEvents::TEvUndelivered::ReasonActorUnknown),
            0, readReq->Cookie), NodeId);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncWithDDiskResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::ERROR);
    }

    Y_UNIT_TEST(SyncWithDDiskViaFakeSource) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(11, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 99;
        const ui32 srcSlotId = 1;
        TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId = MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId);
        ctx.Runtime.RegisterService(fakeSourceServiceId, fakeSourceEdge);

        const TString payload = MakeData('S', BlockSize);
        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(NodeId, srcPDiskId, srcSlotId),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(7, 0, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(readReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetCredentials().GetTabletId(), 50);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }

        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(TReplyStatus::OK, std::nullopt, TRope(payload)),
            0, readReq->Cookie), NodeId);

        auto logIncrement = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logIncrementReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logIncrementReply->Results.emplace_back(logIncrement->Get()->Lsn, logIncrement->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logIncrement, logIncrementReply.release());

        auto logSnapshot = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logSnapshotReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logSnapshotReply->Results.emplace_back(logSnapshot->Get()->Lsn, logSnapshot->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logSnapshot, logSnapshotReply.release());

        auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(2001);
        ctx.SendPDiskResponse(disk, *refill, refillReply.release());

        auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncWithDDiskResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(SyncWithPBViaFakeSource) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(12, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 98;
        const ui32 srcSlotId = 1;
        TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId = MakeBlobStoragePersistentBufferId(NodeId, srcPDiskId, srcSlotId);
        ctx.Runtime.RegisterService(fakeSourceServiceId, fakeSourceEdge);

        const TString payload = MakeData('P', BlockSize);
        auto syncEv = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(
            creds,
            std::make_tuple(NodeId, srcPDiskId, srcSlotId),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(5, 0, BlockSize), 10, 1);

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvReadPersistentBuffer));
        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadPersistentBufferResult(TReplyStatus::OK, std::nullopt,
                5, 0, BlockSize, TRope(payload)),
            0, readReq->Cookie), NodeId);

        auto logIncrement = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logIncrementReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logIncrementReply->Results.emplace_back(logIncrement->Get()->Lsn, logIncrement->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logIncrement, logIncrementReply.release());

        auto logSnapshot = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        auto logSnapshotReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logSnapshotReply->Results.emplace_back(logSnapshot->Get()->Lsn, logSnapshot->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logSnapshot, logSnapshotReply.release());

        auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(3001);
        ctx.SendPDiskResponse(disk, *refill, refillReply.release());

        auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncWithPersistentBufferResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }
}

} // NKikimr
