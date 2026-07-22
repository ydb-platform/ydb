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
#include <ydb/core/blobstorage/ddisk/persistent_buffer_header.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/printf.h>

#include <cstring>

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

    // Create a DDisk instance that simulates a restart where the PB chunks from a
    // previous instance are passed via StartingPoints. Mirrors the helper of the same
    // name in ddisk_actor_batch_write_ut.cpp.
    TDiskHandle CreateDDiskWithRestoredChunkData(ui32 pdiskId, ui32 slotId,
            const std::vector<ui32>& preExistingChunkIds, ui64 oldUniqueId,
            const std::unordered_map<ui32, TString>& chunkData) {
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

        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;

        auto init = WaitPDiskRequest<NPDisk::TEvYardInit>(disk);
        TVector<ui32> ownedChunks;
        auto initReply = std::make_unique<NPDisk::TEvYardInitResult>(
            NKikimrProto::OK,
            0, 0, 0,
            BlockSize, BlockSize, BlockSize,
            ChunkSize,
            BlockSize,
            Owner,
            OwnerRound,
            1,
            0,
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

        {
            NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord pbChunkMap;
            for (ui32 chunkIdx : preExistingChunkIds) {
                pbChunkMap.AddChunkIdxs(chunkIdx);
            }
            pbChunkMap.SetUniqueId(oldUniqueId + 1);

            TString pbChunkMapData;
            const bool serializeOk = pbChunkMap.SerializeToString(&pbChunkMapData);
            Y_ABORT_UNLESS(serializeOk);
            initReply->StartingPoints[TLogSignature::SignaturePersistentBufferChunkMap] =
                NPDisk::TLogRecord(TLogSignature::SignaturePersistentBufferChunkMap,
                                   TRcBuf(pbChunkMapData), 1 /*lsn*/);
        }

        SendPDiskResponse(disk, *init, initReply.release());

        auto readLog = WaitPDiskRequest<NPDisk::TEvReadLog>(disk);
        auto readLogReply = std::make_unique<NPDisk::TEvReadLogResult>(
            NKikimrProto::OK,
            readLog->Get()->Position,
            readLog->Get()->Position,
            true,
            0,
            "",
            Owner);
        SendPDiskResponse(disk, *readLog, readLogReply.release());

        for (ui32 i = 0; i < preExistingChunkIds.size(); ++i) {
            auto readRaw = WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            const ui32 chunkIdx = readRaw->Get()->ChunkIdx;
            auto it = chunkData.find(chunkIdx);
            TString data;
            if (it != chunkData.end()) {
                data = it->second;
            } else {
                data = TString(TTestContext::ChunkSize, '\0');
            }
            SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(data)));
        }

        return disk;
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

    // 9. Persistence: a PB write carrying sender checksums stores them, and an in-memory read
    // (record still cached, no restart) returns exactly those checksums, in order, alongside the
    // original payload.
    Y_UNIT_TEST(SinglePBWriteWithChecksumsReadBackInMemory) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(60, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 110, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('Z', 2 * BlockSize);
        const NDDisk::TBlockSelector selector{5, 0, 2 * BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        const std::vector<ui64> expectedChecksums(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
        UNIT_ASSERT_VALUES_EQUAL(expectedChecksums.size(), 2u);

        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }
    }

    // 10. Persistence across restart, both the direct (non-batch) write path and the batched write
    // path: record A is written directly, B and C are accumulated into a single batch header
    // sector while A's disk write is in-flight. All three carry sender checksums. After a
    // simulated restart (StartingPoints replay from a fresh DDisk instance), all three records
    // must be listed and readable with the same checksums that were originally sent.
    Y_UNIT_TEST(WritesWithChecksumsAndBatchSurviveRestart) {
        TTestContext ctx;

        const TDiskHandle disk1 = ctx.CreateDDisk(61, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 111, 1);

        const ui64 lsnA = 1;
        const ui64 lsnB = 77;
        const ui64 lsnC = 88;
        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const TString payloadC = MakeData('C', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};
        const NDDisk::TBlockSelector selectorC{3, 0, BlockSize};

        // Accumulate all raw PDisk writes into per-chunk buffers so we can feed them back to
        // disk2 during restore.
        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize, "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        std::vector<ui64> checksumsA, checksumsB, checksumsC;

        // Step 1: send A, keep its PDisk write pending so B and C enter the batch path.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorA, lsnA, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payloadA));
            checksumsA.assign(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        const TActorId disk1ActorId = rawA->Sender;
        captureWrite(rawA);

        // Step 2/3: B and C accumulate into the same batch while A is in-flight.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payloadB));
            checksumsB.assign(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payloadC));
            checksumsC.assign(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // Step 4: complete A.
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // Step 5: wakeup fires -> combined write for B and C (header + B data + C data).
        auto rawBC = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC->Get()->Data.size(), 3 * BlockSize,
            "Combined write must contain header + B data + C data (3 sectors)");
        captureWrite(rawBC);

        const ui64 actualUniqueId = [&]() -> ui64 {
            const TString& buf = chunkBufs[rawBC->Get()->ChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(buf.data() + rawBC->Get()->Offset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u, "Header must record BatchSize=2 for B and C");
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->Version, 1u,
                "Header Version must be 1 when any record in the batch carries payload checksums");
            return hdr->PersistentBufferUniqueId;
        }();
        UNIT_ASSERT_C(actualUniqueId != 0, "PersistentBufferUniqueId must not be zero");

        // Step 6: complete the combined write -> B and C both reply.
        ctx.SendPDiskResponse(disk1, *rawBC, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrB, TReplyStatus::OK);
        auto wrC = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrC, TReplyStatus::OK);

        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }

        // Phase 2: restart with instance 2 (same PDiskId, same UniqueId).
        std::optional<TActorId> disk2PdiskEdge;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->Sender == disk1ActorId) {
                if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                    ctx.Runtime.Send(new IEventHandle(ev->Sender, disk1.PDiskEdge,
                        new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                        0, ev->Cookie), NodeId);
                }
                return false;
            }
            if (disk2PdiskEdge &&
                    ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType &&
                    ev->GetRecipientRewrite() == *disk2PdiskEdge) {
                auto* req = ev->Get<NPDisk::TEvChunkReadRaw>();
                const ui32 chunkIdx = req->ChunkIdx;
                const ui32 offset   = req->Offset;
                const ui32 size     = req->Size;
                auto it = chunkBufs.find(chunkIdx);
                TString slice(size, '\0');
                if (it != chunkBufs.end()) {
                    const TString& buf = it->second;
                    UNIT_ASSERT_C(offset + size <= buf.size(), "Read request out of chunk bounds");
                    memcpy(slice.Detach(), buf.data() + offset, size);
                }
                ctx.Runtime.Send(new IEventHandle(ev->Sender, *disk2PdiskEdge,
                    new NPDisk::TEvChunkReadRawResult(TRope(slice)),
                    0, ev->Cookie), NodeId);
                return false;
            }
            return true;
        };

        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            61, 1,
            pbChunkIds,
            /*oldUniqueId=*/actualUniqueId - 1,
            chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;

        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 111, 1);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 3,
            "Records A, B and C must all be restored after restart");

        auto checkRead = [&](const NDDisk::TBlockSelector& selector, ui64 lsn, const TString& payload,
                const std::vector<ui64>& expectedChecksums, const TString& label) {
            auto read = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
                ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selector, lsn, 1, {true}));
            AssertStatus(read, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL_C(read->Get()->GetPayload(0).ConvertToString(), payload,
                TStringBuilder() << label << ": restored payload must match");
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(read->Get()->Record.ChecksumsSize()), expectedChecksums.size(),
                TStringBuilder() << label << ": checksum count mismatch after restore");
            for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(read->Get()->Record.GetChecksums(i), expectedChecksums[i],
                    TStringBuilder() << label << ": checksum " << i << " mismatch after restore");
            }
        };

        checkRead(selectorA, lsnA, payloadA, checksumsA, "record A (non-batch)");
        checkRead(selectorB, lsnB, payloadB, checksumsB, "record B (batched)");
        checkRead(selectorC, lsnC, payloadC, checksumsC, "record C (batched)");
    }

    // 11. Signature-correction sector: the sender's checksum is validated over, and must be
    // persisted as, the ORIGINAL payload bytes -- even though PB zeroes the colliding byte on disk
    // and restores it on read. If the checksum were ever recomputed from the on-disk (corrected)
    // bytes it would not match what the sender sent.
    Y_UNIT_TEST(SignatureCorrectionChecksumPersistedAsOriginal) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(62, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 112, 1);

        const ui64 lsn = 1;
        // The second data sector's first byte deliberately equals the PB header signature byte,
        // forcing TDDiskActor to zero it on disk (HasSignatureCorrection) and restore it on read.
        TString payload = MakeData('S', 2 * BlockSize);
        payload.Detach()[BlockSize] = static_cast<char>(NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature[0]);
        const NDDisk::TBlockSelector selector{7, 0, 2 * BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        const std::vector<ui64> expectedChecksums(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());

        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        // Sanity: the on-disk sector's colliding byte was actually zeroed (header sector + 2 data
        // sectors are written contiguously, so the second data sector starts at offset 2*BlockSize).
        const TString writtenData = pbWriteRaw->Get()->Data.ConvertToString();
        UNIT_ASSERT_VALUES_EQUAL_C(writtenData[2 * BlockSize], '\0',
            "Signature-correction must zero the colliding byte on disk");
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->GetPayload(0).ConvertToString(), payload,
            "Original signature byte must be restored on read");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Record.GetChecksums(i), expectedChecksums[i],
                "Persisted checksum must be the pre-correction (original) value, never recomputed from on-disk bytes");
        }
    }

    // 12. Legacy / no-checksum compatibility across restart: a write without sender checksums
    // (opt-in, same as SinglePBWriteWithoutChecksums) must restore successfully and its read must
    // return an empty Checksums list, both immediately and after a restart.
    Y_UNIT_TEST(LegacyWriteWithoutChecksumsSurvivesRestartWithEmptyChecksums) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(63, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 113, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('L', BlockSize);
        const NDDisk::TBlockSelector selector{4, 0, BlockSize};

        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize, "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayload(TRope(payload));
        // Deliberately not calling AddPayloadThenChecksum(): checksum persistence is opt-in.
        UNIT_ASSERT_VALUES_EQUAL(write->Record.ChecksumsSize(), 0u);

        SendToDDisk(ctx, disk1.PBServiceId, write.release());

        auto rawWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        const TActorId disk1SenderActorId = rawWrite->Sender;
        captureWrite(rawWrite);
        const ui64 actualUniqueId = [&]() -> ui64 {
            const TString& buf = chunkBufs[rawWrite->Get()->ChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(buf.data() + rawWrite->Get()->Offset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->Version, 0u, "Header Version must stay 0 for a checksum-less write");
            return hdr->PersistentBufferUniqueId;
        }();
        ctx.SendPDiskResponse(disk1, *rawWrite, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }

        std::optional<TActorId> disk2PdiskEdge;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->Sender == disk1SenderActorId) {
                if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                    ctx.Runtime.Send(new IEventHandle(ev->Sender, disk1.PDiskEdge,
                        new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                        0, ev->Cookie), NodeId);
                }
                return false;
            }
            if (disk2PdiskEdge &&
                    ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType &&
                    ev->GetRecipientRewrite() == *disk2PdiskEdge) {
                auto* req = ev->Get<NPDisk::TEvChunkReadRaw>();
                const ui32 chunkIdx = req->ChunkIdx;
                const ui32 offset   = req->Offset;
                const ui32 size     = req->Size;
                auto it = chunkBufs.find(chunkIdx);
                TString slice(size, '\0');
                if (it != chunkBufs.end()) {
                    const TString& buf = it->second;
                    UNIT_ASSERT_C(offset + size <= buf.size(), "Read request out of chunk bounds");
                    memcpy(slice.Detach(), buf.data() + offset, size);
                }
                ctx.Runtime.Send(new IEventHandle(ev->Sender, *disk2PdiskEdge,
                    new NPDisk::TEvChunkReadRawResult(TRope(slice)),
                    0, ev->Cookie), NodeId);
                return false;
            }
            return true;
        };

        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            63, 1,
            pbChunkIds,
            /*oldUniqueId=*/actualUniqueId - 1,
            chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;

        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 113, 1);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->GetPayload(0).ConvertToString(), payload,
            "Legacy record's payload must survive restart");
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), 0u,
            "A checksum-less record must return an empty Checksums list after restore");
    }

    // 13. Re-replication (TEvReadThenWritePersistentBuffers) must forward the checksums that were
    // persisted on the source record, not silently drop them: the coordinator PB reads its own
    // record (which carries sender-supplied checksums) and re-writes it to the target PBs via
    // TEvWritePersistentBuffers -- the checksums must reach and be validated/persisted on every
    // target, exactly as if the original sender had written them directly. Modeled on
    // PersistentBufferReadThenWriteTunnel (ddisk_actor_ut.cpp).
    Y_UNIT_TEST(ReadThenWriteForwardsPersistedChecksums) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(64, 1);
        const TDiskHandle disk2 = ctx.CreateDDisk(65, 1);
        const TDiskHandle disk3 = ctx.CreateDDisk(66, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk1.PBServiceId, 120, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', 2 * BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, 2 * BlockSize};

        auto pbs = std::vector<std::tuple<ui32, ui32, ui32>>{
            {NodeId, disk2.PDiskId, disk2.SlotId},
            {NodeId, disk3.PDiskId, disk3.SlotId}};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        const std::vector<ui64> expectedChecksums(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
        UNIT_ASSERT_VALUES_EQUAL(expectedChecksums.size(), 2u);

        SendToDDisk(ctx, disk1.PBServiceId, write.release());
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        ctx.SendPDiskResponse(disk1, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto readThenWrite = std::make_unique<NDDisk::TEvReadThenWritePersistentBuffers>(creds, lsn, 1, pbs, 1000);
        SendToDDisk(ctx, disk1.PBServiceId, readThenWrite.release());
        for (int i = 0; i < 2; ++i) {
            auto peerWriteRaw = ctx.WaitPDiskRequests<NPDisk::TEvChunkWriteRaw>({disk2.PDiskEdge, disk3.PDiskEdge});
            const TDiskHandle& respondingDisk = (peerWriteRaw->Recipient == disk2.PDiskEdge) ? disk2 : disk3;
            ctx.SendPDiskResponse(respondingDisk, *peerWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }

        auto tunnelResult = ctx.Runtime.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBuffersResult>(ctx.Edge, false);
        UNIT_ASSERT_VALUES_EQUAL(tunnelResult->Get()->Record.ResultSize(), 2u);
        for (ui32 i = 0; i < tunnelResult->Get()->Record.ResultSize(); ++i) {
            const auto& wr = tunnelResult->Get()->Record.GetResult(i);
            UNIT_ASSERT_C(wr.GetResult().GetStatus() == TReplyStatus::OK, TStringBuilder()
                << "Re-replicated write " << i << " expected OK, got "
                << TReplyStatus::E_Name(static_cast<TReplyStatus::E>(wr.GetResult().GetStatus())));
        }

        for (auto disk : {disk2, disk3}) {
            NDDisk::TQueryCredentials peerCreds = Connect(ctx, disk.PBServiceId, 120, 1);
            auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(peerCreds, selector, lsn, 1, {true}));
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->GetPayload(0).ConvertToString(), payload,
                "Re-replicated payload must match the original");
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), expectedChecksums.size(),
                "Re-replicated record must carry the same checksums as the original write, not an empty list");
            for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Record.GetChecksums(i), expectedChecksums[i],
                    "Checksum " << i << " must survive TEvReadThenWritePersistentBuffers re-replication");
            }
        }
    }

    // 14. Batch header overflow: checksummed 8-sector records (the largest size still eligible for
    // batching) accumulate in an open batch until one more record's metadata (TPersistentBufferLsnRecordHeader
    // + its sector-location array + one checksum per data sector, see LsnRecordMetadataSize) no longer
    // fits the shared 4 KiB header sector -- even though the batch's outer record-count limit
    // (MaxLsnsPerPack, sized for checksum-less records) is not yet reached. That overflowing record
    // must fall back to a direct (own-header) write instead of corrupting the header sector, and every
    // record -- batched and direct alike -- must restore with correct payload and checksums after a
    // restart.
    Y_UNIT_TEST(BatchHeaderOverflowFallsBackToDirectWrite) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(67, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 121, 1);

        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize, "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        // Write A: keep-alive PDisk write; keeps PersistentBufferDiskOperationInflight non-empty so
        // subsequent small writes take the batch path instead of a direct write.
        const TString payloadA = MakeData('A', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        captureWrite(rawA);

        // Compute, the same way LsnRecordMetadataSize does, exactly how many checksummed 8-sector
        // records fit in the shared 4 KiB header sector alongside TPersistentBufferHeader: that many
        // are sent first (all must batch, i.e. produce no PDisk write of their own), then one more is
        // sent (must overflow and fall back to a direct write). This is deterministic -- no need to
        // race for "did a write appear immediately", since A's own raw write was already dequeued above
        // and batched records never produce a raw write, so the next TEvChunkWriteRaw on disk1 can only
        // be the overflowing record's direct write.
        constexpr ui32 kSectorsPerRecord = 8;
        const ui32 recordMetadataSize = sizeof(NDDisk::TPersistentBufferLsnRecordHeader)
            + kSectorsPerRecord * sizeof(NDDisk::TPersistentBufferSectorInfo)
            + kSectorsPerRecord * sizeof(ui64);
        const ui32 headerBudget = BlockSize - sizeof(NDDisk::TPersistentBufferHeader);
        const ui32 batchedCount = headerBudget / recordMetadataSize;
        UNIT_ASSERT_C(batchedCount >= 1, "Sanity: at least one checksummed 8-sector record must fit the header sector");

        struct TRecordInfo {
            TString Payload;
            std::vector<ui64> Checksums;
            NDDisk::TBlockSelector Selector;
            ui64 Lsn;
        };
        auto makeRecord = [&](ui32 i) {
            TString payload = MakeData(static_cast<char>('a' + (i % 26)), kSectorsPerRecord * BlockSize);
            const NDDisk::TBlockSelector selector{ui64(10 + i), 0, kSectorsPerRecord * BlockSize};
            const ui64 lsn = 100 + i;
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            TRecordInfo info{payload,
                std::vector<ui64>(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end()),
                selector, lsn};
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
            return info;
        };

        std::vector<TRecordInfo> batched;
        for (ui32 i = 0; i < batchedCount; ++i) {
            batched.push_back(makeRecord(i));
        }
        const TRecordInfo overflow = makeRecord(batchedCount);

        auto rawOverflow = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawOverflow->Get()->Data.size(), (kSectorsPerRecord + 1) * BlockSize,
            "Overflowing record must fall back to its own direct (header + data) write");
        captureWrite(rawOverflow);
        ctx.SendPDiskResponse(disk1, *rawOverflow, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto overflowResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(overflowResult, TReplyStatus::OK);

        // Complete A's write, then the batch's combined write (header + all batched records' data).
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        auto rawBatch = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBatch->Get()->Data.size(), (1 + batched.size() * kSectorsPerRecord) * BlockSize,
            "Batch combined write must contain the header sector plus every batched record's data sectors");
        const ui64 actualUniqueId = [&]() -> ui64 {
            captureWrite(rawBatch);
            const TString& buf = chunkBufs[rawBatch->Get()->ChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(buf.data() + rawBatch->Get()->Offset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, static_cast<ui32>(batched.size()),
                "BatchSize must equal the number of successfully batched records");
            return hdr->PersistentBufferUniqueId;
        }();
        ctx.SendPDiskResponse(disk1, *rawBatch, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        for (ui32 i = 0; i < batched.size(); ++i) {
            auto wr = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(wr, TReplyStatus::OK);
        }

        // Restart: restore from the captured chunk data and verify every record -- the overflowing
        // direct write and every batched record -- comes back with the correct payload and checksums.
        // Reading a restored record fetches its data sectors from PDisk on demand (restore only
        // reconstructs in-memory metadata, not payload bytes), so intercept TEvChunkReadRaw to disk2
        // and serve it from the captured chunkBufs; also auto-answer TEvCheckSpace, which the actor
        // may still emit periodically.
        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }
        std::optional<TActorId> disk2PdiskEdge;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                ctx.Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                    0, ev->Cookie), NodeId);
                return false;
            }
            if (disk2PdiskEdge &&
                    ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType &&
                    ev->GetRecipientRewrite() == *disk2PdiskEdge) {
                auto* req = ev->Get<NPDisk::TEvChunkReadRaw>();
                const ui32 chunkIdx = req->ChunkIdx;
                const ui32 offset   = req->Offset;
                const ui32 size     = req->Size;
                auto it = chunkBufs.find(chunkIdx);
                TString slice(size, '\0');
                if (it != chunkBufs.end()) {
                    const TString& buf = it->second;
                    UNIT_ASSERT_C(offset + size <= buf.size(), "Read request out of chunk bounds");
                    memcpy(slice.Detach(), buf.data() + offset, size);
                }
                ctx.Runtime.Send(new IEventHandle(ev->Sender, *disk2PdiskEdge,
                    new NPDisk::TEvChunkReadRawResult(TRope(slice)),
                    0, ev->Cookie), NodeId);
                return false;
            }
            return true;
        };

        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            67, 1, pbChunkIds, /*oldUniqueId=*/actualUniqueId - 1, chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;
        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 121, 1);

        auto checkRecord = [&](const TRecordInfo& info, const TString& label) {
            auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
                ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, info.Selector, info.Lsn, 1, {true}));
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->GetPayload(0).ConvertToString(), info.Payload,
                label << " payload mismatch");
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), info.Checksums.size(),
                label << " checksum count mismatch");
            for (ui32 i = 0; i < info.Checksums.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Record.GetChecksums(i), info.Checksums[i],
                    label << " checksum " << i << " mismatch");
            }
        };

        checkRecord(overflow, "overflow (direct) record");
        for (ui32 i = 0; i < batched.size(); ++i) {
            checkRecord(batched[i], TStringBuilder() << "batched record " << i);
        }
    }

    // 15. Partial-range read: a read selector narrower than the written record (and starting at a
    // nonzero offset within it) must return both the correct payload sub-range and the matching
    // checksum sub-range -- exercising the firstBlock/blockCount slicing in ReplyReadPersistentBuffer.
    Y_UNIT_TEST(PartialRangeReadSlicesChecksums) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(68, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 122, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('R', 4 * BlockSize);
        const NDDisk::TBlockSelector selector{5, 0, 4 * BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        const std::vector<ui64> allChecksums(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
        UNIT_ASSERT_VALUES_EQUAL(allChecksums.size(), 4u);

        SendToDDisk(ctx, disk.PBServiceId, write.release());
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        // Read blocks [1, 3) out of the 4-block record, i.e. a 2-block selector starting one block in.
        const NDDisk::TBlockSelector partialSelector{5, BlockSize, 2 * BlockSize};
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, partialSelector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->GetPayload(0).ConvertToString(), payload.substr(BlockSize, 2 * BlockSize),
            "Partial read must return only the requested payload sub-range");
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readResult->Get()->Record.ChecksumsSize()), 2u,
            "Partial read must return only the checksums for the requested sub-range, not all 4");
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Record.GetChecksums(0), allChecksums[1],
            "First returned checksum must be the record's block 1 (not block 0)");
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Record.GetChecksums(1), allChecksums[2],
            "Second returned checksum must be the record's block 2");
    }

    // 16. Mixed batch: a checksummed record and a checksum-less record share the same batch header.
    // After a restart, the checksummed record must restore with its checksums and the checksum-less
    // one must restore with an empty Checksums list -- exercising the per-record HAS_PAYLOAD_CHECKSUMS
    // flag check during batch restore parsing, as opposed to a single Version-wide switch.
    Y_UNIT_TEST(MixedChecksummedAndLegacyBatchRestoresCorrectly) {
        TTestContext ctx;
        const TDiskHandle disk1 = ctx.CreateDDisk(69, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 123, 1);

        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize, "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        // Write A: keep-alive PDisk write; keeps PersistentBufferDiskOperationInflight non-empty so
        // subsequent small writes take the batch path.
        const TString payloadA = MakeData('A', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        captureWrite(rawA);

        // Write B: batched, WITHOUT checksums (legacy/opt-out).
        const TString payloadB = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // Write C: batched, WITH checksums -- lands in the same batch header as B.
        const TString payloadC = MakeData('C', BlockSize);
        const NDDisk::TBlockSelector selectorC{3, 0, BlockSize};
        std::vector<ui64> checksumsC;
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selectorC, 3, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payloadC));
            checksumsC.assign(write->Record.GetChecksums().begin(), write->Record.GetChecksums().end());
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        UNIT_ASSERT_VALUES_EQUAL(checksumsC.size(), 1u);

        // Complete A's write, then the batch's combined write (header + B's + C's data sectors).
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        auto rawBatch = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBatch->Get()->Data.size(), 3 * BlockSize,
            "Batch combined write must contain the header sector plus B's and C's data sectors");
        const ui64 actualUniqueId = [&]() -> ui64 {
            captureWrite(rawBatch);
            const TString& buf = chunkBufs[rawBatch->Get()->ChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(buf.data() + rawBatch->Get()->Offset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u, "Both B and C must land in the same batch");
            return hdr->PersistentBufferUniqueId;
        }();
        ctx.SendPDiskResponse(disk1, *rawBatch, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrB, TReplyStatus::OK);
        auto wrC = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrC, TReplyStatus::OK);

        // Restart. Reading a restored record fetches its data sectors from PDisk on demand, so
        // intercept TEvChunkReadRaw to disk2 and serve it from the captured chunkBufs; also
        // auto-answer TEvCheckSpace, which the actor may still emit periodically.
        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }
        std::optional<TActorId> disk2PdiskEdge;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                ctx.Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                    0, ev->Cookie), NodeId);
                return false;
            }
            if (disk2PdiskEdge &&
                    ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType &&
                    ev->GetRecipientRewrite() == *disk2PdiskEdge) {
                auto* req = ev->Get<NPDisk::TEvChunkReadRaw>();
                const ui32 chunkIdx = req->ChunkIdx;
                const ui32 offset   = req->Offset;
                const ui32 size     = req->Size;
                auto it = chunkBufs.find(chunkIdx);
                TString slice(size, '\0');
                if (it != chunkBufs.end()) {
                    const TString& buf = it->second;
                    UNIT_ASSERT_C(offset + size <= buf.size(), "Read request out of chunk bounds");
                    memcpy(slice.Detach(), buf.data() + offset, size);
                }
                ctx.Runtime.Send(new IEventHandle(ev->Sender, *disk2PdiskEdge,
                    new NPDisk::TEvChunkReadRawResult(TRope(slice)),
                    0, ev->Cookie), NodeId);
                return false;
            }
            return true;
        };

        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            69, 1, pbChunkIds, /*oldUniqueId=*/actualUniqueId - 1, chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;
        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 123, 1);

        auto readB = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selectorB, 2, 1, {true}));
        AssertStatus(readB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readB->Get()->GetPayload(0).ConvertToString(), payloadB,
            "Checksum-less batched record's payload must survive restart");
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readB->Get()->Record.ChecksumsSize()), 0u,
            "Checksum-less batched record must restore with an empty Checksums list");

        auto readC = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selectorC, 3, 1, {true}));
        AssertStatus(readC, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readC->Get()->GetPayload(0).ConvertToString(), payloadC,
            "Checksummed batched record's payload must survive restart");
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(readC->Get()->Record.ChecksumsSize()), checksumsC.size(),
            "Checksummed batched record must restore with its checksums, not an empty list");
        for (ui32 i = 0; i < checksumsC.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(readC->Get()->Record.GetChecksums(i), checksumsC[i],
                "Checksum " << i << " must survive restart for the checksummed batched record");
        }
    }
}

} // anonymous namespace
} // namespace NKikimr
