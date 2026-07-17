// Tests for the sender-supplied per-block payload checksum piggybacked on the PB write path
// (TEvWritePersistentBuffer / TEvWritePersistentBuffers Checksums field, see RFC 006).
//
// The PB validates the checksum before writing anything to disk and rejects a mismatch with
// TReplyStatus::CORRUPTED, without ever touching PDisk. For TEvWritePersistentBuffers, the
// coordinator (TWritePersistentBuffersRequestActor) forwards the sender's checksums verbatim to
// every fanned-out TEvWritePersistentBuffer; each peer PB validates independently and reports its
// own status back — the coordinator does not special-case CORRUPTED in any way.

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace {

using NKikimrBlobStorage::NDDisk::TReplyStatus;

constexpr ui32 NodeId = 1;
constexpr ui32 BlockSize = 4096;
constexpr ui32 MinChunksReserved = 2;
constexpr ui32 PersistentBufferInitChunks = 4;

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
        NDDisk::TPersistentBufferFormat pbFormat{256, 4, BlockSize * 128, 8, 5000, 512 * 1024};
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

NDDisk::TQueryCredentials Connect(TTestContext& ctx, const TActorId& serviceId, ui64 tabletId, ui32 generation) {
    NDDisk::TQueryCredentials creds;
    creds.TabletId = tabletId;
    creds.Generation = generation;

    auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, serviceId, new NDDisk::TEvConnect(creds));
    AssertStatus(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

    return creds;
}

// Asserts that no disk write was issued to any of the given PDisk edges: a fresh sentinel edge is
// sent a self-message, and the first of {edges..., sentinel} to actually arrive must be the
// sentinel (i.e. nothing beat it from any PDisk edge).
void AssertNoDiskWrite(TTestContext& ctx, const std::set<TActorId>& pdiskEdges) {
    TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
    ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
    std::set<TActorId> waitFor = pdiskEdges;
    waitFor.insert(sentinelEdge);
    auto ev = ctx.Runtime.WaitForEdgeActorEvent(waitFor);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
        "checksum-mismatched write must not issue any disk write");
}

// Walks the same counters subgroup chain TDDiskActor builds (see TDDiskActor::TDDiskActor and
// TDDiskActor::RegisterCounters -- the "counters"/"ddisks" layer comes from
// GetServiceCounters(CountersParent, "ddisks") -- in ddisk_actor.cpp) down to the "checksums"
// subsystem for the given disk, and returns the named counter. Every disk in this file is created
// via CreateDDisk() with a single-VDisk-per-group TBlobStorageGroupInfo and the default groupId, so
// "group" and "orderNumber" are always 0; only "pdisk" differs between disks sharing the same
// TTestContext::Counters root.
NMonitoring::TDynamicCounters::TCounterPtr GetChecksumCounter(TTestContext& ctx, const TDiskHandle& disk,
        const TString& name) {
    return ctx.Counters
        ->GetSubgroup("counters", "ddisks")
        ->GetSubgroup("ddiskPool", "ddisk_pool")
        ->GetSubgroup("group", Sprintf("%09u", 0u))
        ->GetSubgroup("orderNumber", Sprintf("%02u", 0u))
        ->GetSubgroup("pdisk", Sprintf("%09u", disk.PDiskId))
        ->GetSubgroup("media", "nvme")
        ->GetSubgroup("subsystem", "checksums")
        ->GetCounter(name, true);
}

Y_UNIT_TEST_SUITE(TDDiskChecksumTests) {

    // 1. Single PB write with a single 4 KiB block: a mismatched checksum must be rejected with
    // CORRUPTED and must not reach PDisk at all.
    Y_UNIT_TEST(SinglePBWriteChecksumMismatchOneBlock) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(50, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 90, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        write->Record.SetChecksums(0, write->Record.GetChecksums(0) + 1);

        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::CORRUPTED);

        AssertNoDiskWrite(ctx, {disk.PDiskEdge});
        UNIT_ASSERT_VALUES_EQUAL(GetChecksumCounter(ctx, disk, "ChecksumMismatch")->Val(), 1);
    }

    // 2. Single PB write with 4 4 KiB blocks: for each subcase exactly one block's checksum is
    // wrong (first/second/third/last), the whole write must still be rejected with CORRUPTED.
    Y_UNIT_TEST(SinglePBWriteChecksumMismatchFourBlocksSubcases) {
        for (ui32 corruptedBlock = 0; corruptedBlock < 4; ++corruptedBlock) {
            TTestContext ctx;
            const TDiskHandle disk = ctx.CreateDDisk(50, 1);
            NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 90, 1);

            const ui64 lsn = 1;
            const TString payload = MakeData('Q', 4 * BlockSize);
            const NDDisk::TBlockSelector selector{3, 0, 4 * BlockSize};

            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            write->Record.SetChecksums(corruptedBlock, write->Record.GetChecksums(corruptedBlock) + 1);

            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::CORRUPTED);

            AssertNoDiskWrite(ctx, {disk.PDiskEdge});
        }
    }

    // 3. Write to multiple persistent buffers (TEvWritePersistentBuffers), single 4 KiB block:
    // a checksum mismatch on the record received by the coordinator is forwarded verbatim to every
    // fanned-out peer, so every peer independently detects it and reports CORRUPTED; nothing is
    // ever written to any of the three PDisks.
    Y_UNIT_TEST(MultiPBWriteChecksumMismatchOneBlock) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{
            {NodeId, disk1.PDiskId, disk1.SlotId},
            {NodeId, disk2.PDiskId, disk2.SlotId},
            {NodeId, disk3.PDiskId, disk3.SlotId}};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn,
            NDDisk::TWriteInstruction(0), pbs, 1000);
        write->AddPayloadThenChecksum(TRope(payload));
        write->Record.SetChecksums(0, write->Record.GetChecksums(0) + 1);

        SendToDDisk(ctx, disk1.PBServiceId, write.release());

        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(ctx.Edge, false);
        UNIT_ASSERT_VALUES_EQUAL(writeResult->Get()->Record.ResultSize(), 3u);
        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); ++i) {
            const auto& wr = writeResult->Get()->Record.GetResult(i);
            const auto status = static_cast<TReplyStatus::E>(wr.GetResult().GetStatus());
            UNIT_ASSERT_C(status == TReplyStatus::CORRUPTED, TStringBuilder()
                << "PB reply " << i << " expected CORRUPTED, got " << TReplyStatus::E_Name(status));
        }

        AssertNoDiskWrite(ctx, {disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
    }

    // 3b/2 combined for multi-PB: 4 blocks, one bad block per subcase; every peer must reject.
    Y_UNIT_TEST(MultiPBWriteChecksumMismatchFourBlocksSubcases) {
        for (ui32 corruptedBlock = 0; corruptedBlock < 4; ++corruptedBlock) {
            TTestContext ctx;
            const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
            const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
            const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
            NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);

            const ui64 lsn = 10;
            const TString payload = MakeData('P', 4 * BlockSize);
            const NDDisk::TBlockSelector selector{3, 0, 4 * BlockSize};

            auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{
                {NodeId, disk1.PDiskId, disk1.SlotId},
                {NodeId, disk2.PDiskId, disk2.SlotId},
                {NodeId, disk3.PDiskId, disk3.SlotId}};
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn,
                NDDisk::TWriteInstruction(0), pbs, 1000);
            write->AddPayloadThenChecksum(TRope(payload));
            write->Record.SetChecksums(corruptedBlock, write->Record.GetChecksums(corruptedBlock) + 1);

            SendToDDisk(ctx, disk1.PBServiceId, write.release());

            auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(ctx.Edge, false);
            UNIT_ASSERT_VALUES_EQUAL(writeResult->Get()->Record.ResultSize(), 3u);
            for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); ++i) {
                const auto& wr = writeResult->Get()->Record.GetResult(i);
                const auto status = static_cast<TReplyStatus::E>(wr.GetResult().GetStatus());
                UNIT_ASSERT_C(status == TReplyStatus::CORRUPTED, TStringBuilder()
                    << "corruptedBlock=" << corruptedBlock << " PB reply " << i
                    << " expected CORRUPTED, got " << TReplyStatus::E_Name(status));
            }

            AssertNoDiskWrite(ctx, {disk1.PDiskEdge, disk2.PDiskEdge, disk3.PDiskEdge});
        }
    }

    // 4. Write to multiple PBs where everything the sender submitted is correct, but the payload
    // gets corrupted specifically on the hop between the coordinator (the PB acting as
    // TWritePersistentBuffersRequestActor) and one of the fanned-out peer PBs. The PB has no special
    // handling for this: it just detects the mismatch itself and reports CORRUPTED like any other
    // write failure, while the other (uncorrupted) peers succeed normally.
    Y_UNIT_TEST(MultiPBWriteCorruptedBetweenCoordinatorAndPeer) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(6, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(7, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(8, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{
            {NodeId, disk1.PDiskId, disk1.SlotId},
            {NodeId, disk2.PDiskId, disk2.SlotId},
            {NodeId, disk3.PDiskId, disk3.SlotId}};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffers>(creds, selector, lsn,
            NDDisk::TWriteInstruction(0), pbs, 1000);
        write->AddPayloadThenChecksum(TRope(payload)); // correct: the coordinator forwards these checksums verbatim to every peer

        // The coordinator assigns fan-out cookies 1, 2, 3 to disk1, disk2, disk3 respectively (in
        // TEvWritePersistentBuffers order); cookie 2 is the message routed to disk2. Flip one of its
        // checksums in place to emulate corruption in transit between the coordinator and that peer.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted && ev->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffer::EventType && ev->Cookie == 2) {
                intercepted = true;
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TEvWritePersistentBuffer>*>(ev.get());
                orig->Get()->Record.SetChecksums(0, orig->Get()->Record.GetChecksums(0) + 1);
            }
            return true;
        };

        SendToDDisk(ctx, disk1.PBServiceId, write.release());

        // disk1 and disk3 receive the payload intact and proceed to a normal disk write; disk2
        // rejects immediately with CORRUPTED and never touches PDisk.
        for (int i = 0; i < 2; ++i) {
            auto pbWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk1.PDiskEdge, disk3.PDiskEdge});
            // Reply from whichever of disk1/disk3 actually received this request: the response's
            // "Sender" field should reflect the real PDisk edge, not always disk1's.
            const TDiskHandle& respondingDisk = (pbWriteRaw->Recipient == disk1.PDiskEdge) ? disk1 : disk3;
            ctx.SendPDiskResponse(respondingDisk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }

        auto writeResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(ctx.Edge, false);
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_C(intercepted, "Filter must have fired for the disk2 peer write");
        UNIT_ASSERT_VALUES_EQUAL(writeResult->Get()->Record.ResultSize(), 3u);

        for (ui32 i = 0; i < writeResult->Get()->Record.ResultSize(); ++i) {
            const auto& wr = writeResult->Get()->Record.GetResult(i);
            const auto& pbId = wr.GetPersistentBufferId();
            const auto status = static_cast<TReplyStatus::E>(wr.GetResult().GetStatus());
            if (pbId.GetPDiskId() == disk2.PDiskId) {
                UNIT_ASSERT_C(status == TReplyStatus::CORRUPTED,
                    "disk2 (corrupted in transit) must report CORRUPTED, got " << TReplyStatus::E_Name(status));
            } else {
                UNIT_ASSERT_C(status == TReplyStatus::OK,
                    "disk1/disk3 (untouched) must succeed, got " << TReplyStatus::E_Name(status));
            }
        }
    }

    // 5. PB write where the sender's Checksums count does not match the payload size (in MinSectorSize
    // blocks): rejected with INCORRECT_REQUEST before touching PDisk, distinct from a CORRUPTED value
    // mismatch.
    Y_UNIT_TEST(SinglePBWriteChecksumCountMismatch) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(50, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 90, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('P', 2 * BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, 2 * BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        UNIT_ASSERT_VALUES_EQUAL(write->Record.ChecksumsSize(), 2u);
        write->Record.MutableChecksums()->RemoveLast(); // now only 1 checksum for a 2-block payload

        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);

        AssertNoDiskWrite(ctx, {disk.PDiskEdge});
    }

    // 6. Plain TEvWrite with a mismatched checksum: rejected with CORRUPTED and never reaches PDisk,
    // mirroring the TEvWritePersistentBuffer behavior (see Handle(TEvWrite) in ddisk_actor_read_write.cpp).
    Y_UNIT_TEST(PlainWriteChecksumMismatch) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(51, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 91, 1);

        const TString payload = MakeData('W', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWrite>(creds, selector, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(payload));
        write->Record.SetChecksums(0, write->Record.GetChecksums(0) + 1);

        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::CORRUPTED);

        AssertNoDiskWrite(ctx, {disk.PDiskEdge});
        UNIT_ASSERT_VALUES_EQUAL(GetChecksumCounter(ctx, disk, "ChecksumMismatch")->Val(), 1);
    }

    // 7. Plain TEvWrite where the sender's Checksums count does not match the payload size: rejected
    // with INCORRECT_REQUEST and never reaches PDisk.
    Y_UNIT_TEST(PlainWriteChecksumCountMismatch) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(51, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 92, 1);

        const TString payload = MakeData('X', 2 * BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, 2 * BlockSize};

        auto write = std::make_unique<NDDisk::TEvWrite>(creds, selector, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(payload));
        UNIT_ASSERT_VALUES_EQUAL(write->Record.ChecksumsSize(), 2u);
        write->Record.MutableChecksums()->RemoveLast(); // now only 1 checksum for a 2-block payload

        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);

        AssertNoDiskWrite(ctx, {disk.PDiskEdge});
    }

    // 8. External (non-internal) PB write with no Checksums attached at all: validation is opt-in, so
    // this is skipped entirely, the write proceeds and succeeds normally, and WritesWithoutChecksums is
    // incremented to track how common this still is for non-internal senders.
    Y_UNIT_TEST(SinglePBWriteWithoutChecksums) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(52, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 93, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('N', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        // Deliberately not calling AddPayloadThenChecksum(): checksum validation is opt-in.
        UNIT_ASSERT_VALUES_EQUAL(write->Record.ChecksumsSize(), 0u);

        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        UNIT_ASSERT_VALUES_EQUAL(GetChecksumCounter(ctx, disk, "WritesWithoutChecksums")->Val(), 1);
    }
}

} // anonymous namespace
} // namespace NKikimr
