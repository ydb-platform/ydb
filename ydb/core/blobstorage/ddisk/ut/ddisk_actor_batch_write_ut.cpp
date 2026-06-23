// Tests for ProcessPersistentBufferBatchWrite:
// verifies that data is written and restored correctly with a unified header sector.
//
// The batch-write path is activated when:
//   - selector.Size <= MaxSectorsPerPackBufferRecord * SectorSize (≤ 8 sectors)
//   - WritesBatchingPeriodMicroseconds > 0
//   - PendingPersistentBufferEvents.size() < MaxPendingEventsQueueSize
//   - PersistentBufferBatchWriteCookie != 0 OR PersistentBufferDiskOperationInflight.size() > 0
//
// Layout on disk:
//   Sector 0 (header sector, shared by all records in the batch):
//     TPersistentBufferHeader { BatchSize = N, ... }
//     N × TPersistentBufferLsnRecordHeader
//     N × (sectorsCnt × TPersistentBufferSectorInfo)   [data sector locations]
//   Sectors 1..K (data sectors for each record)
//
// NEW behavior (after batch-write refactor):
//   ProcessPersistentBufferBatchWriteData() does NOT issue any disk writes.
//   All data (payload bytes) are accumulated in inflight.DataToWrite (a TRope).
//   The header sector is the FIRST sector of DataToWrite (pre-allocated as a placeholder).
//   ProcessPersistentBufferBatchWrite() is called on the wakeup timer and:
//     - fills the header sector with metadata
//     - frees unused pre-allocated sectors
//     - calls SlicePersistentBufferData() and issues ALL disk writes at once
//       (header + all data sectors in one or more TEvChunkWriteRaw events)
//
// Correct test sequence for a single batched record B:
//   1. Send write A → PDisk write arrives (2 sectors: header + data)
//      (keep it pending — A is in-flight, which activates the batch path)
//   2. Send write B while A is still in-flight → B enters batch path,
//      data is accumulated internally; NO immediate PDisk write for B.
//   3. Complete A's PDisk write → A replies.
//   4. Wakeup timer fires → ProcessPersistentBufferBatchWrite writes
//      a single combined PDisk write: header sector + B's data sector (2 sectors).
//   5. Complete the combined write → B replies.

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

#include <algorithm>
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
    // previous instance are passed via StartingPoints.
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
            TTestContext::ChunkSize,
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

NDDisk::TQueryCredentials Connect(TTestContext& ctx, const TActorId& serviceId, ui64 tabletId, ui32 generation) {
    NDDisk::TQueryCredentials creds;
    creds.TabletId = tabletId;
    creds.Generation = generation;

    auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, serviceId, new NDDisk::TEvConnect(creds));
    AssertStatus(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();

    return creds;
}

// Helper: query FreeSectors from the PB actor.
ui32 GetPBFreeSectors(TTestContext& ctx, const TDiskHandle& disk) {
    SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvGetPersistentBufferInfo(false, false));
    auto info = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);
    return info->Get()->FreeSectors;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDDiskActorBatchWriteTest) {

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: two small writes are batched; all data is flushed in one write.
    //
    // NEW behavior: ProcessPersistentBufferBatchWriteData() does NOT write to disk.
    // All data (payload bytes) are accumulated internally.
    // ProcessPersistentBufferBatchWrite() (wakeup timer) issues ONE combined write:
    //   header sector + B's data sector.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (2 sectors: header + data).
    //      (keep it pending — A is in-flight, which activates the batch path)
    //   2. Send write B while A is in-flight → B enters batch path.
    //      NO immediate PDisk write for B (data accumulated internally).
    //   3. Complete A's PDisk write → A replies.
    //   4. Wakeup timer fires → ProcessPersistentBufferBatchWrite issues ONE
    //      combined write: header sector + B's data sector (2 * BlockSize total).
    //   5. Complete the combined write → B's reply arrives.
    //   6. Verify both records are readable and their data is correct.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteTwoRecordsShareHeaderSector) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(40, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 80, 1);

        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};

        // ── Step 1: send write A (non-batch path) — keep PDisk write pending ──
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        // A's PDisk write arrives: header + 1 data sector = 2 * BlockSize.
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawA->Get()->Data.size(), 2 * BlockSize,
            "First (non-batch) write must produce header + data sectors");

        // ── Step 2: send write B while A is still in-flight (batch path) ─────
        // B's data is accumulated internally; NO immediate PDisk write for B.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: wakeup fires → ProcessPersistentBufferBatchWrite ─────────
        // One combined write: header sector (sector 0) + B's data sector (sector 1).
        auto rawB_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_combined->Get()->Data.size(), 2 * BlockSize,
            "Batch combined write must contain header sector + B's data sector (2 sectors total)");

        // Verify the header sector (first BlockSize bytes) starts with the PB signature.
        {
            TString rawData = rawB_combined->Get()->Data.ConvertToString();
            UNIT_ASSERT_C(
                memcmp(rawData.data(),
                    NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature,
                    sizeof(NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature)) == 0,
                "First sector of combined write must start with PersistentBufferHeaderSignature");
            // BatchSize must be 1 (only record B is in this batch).
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(rawData.data());
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 1u,
                "BatchSize in header must equal the number of batched records");
        }

        // ── Step 5: complete the combined write → B's reply arrives ──────────
        ctx.SendPDiskResponse(disk, *rawB_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);

        // ── Step 6: verify both records are readable ──────────────────────────
        auto readA = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds, selectorA, 1, 1, {true}));
        AssertStatus(readA, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readA->Get()->GetPayload(0).ConvertToString(), payloadA);

        auto readB = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds, selectorB, 2, 1, {true}));
        AssertStatus(readB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readB->Get()->GetPayload(0).ConvertToString(), payloadB);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: batch write reply is deferred until the combined write completes.
    //
    // NEW behavior: B does NOT trigger any PDisk write during accumulation.
    // The combined write (header + B's data) is issued at wakeup time.
    // B must NOT receive a reply before the combined write completes.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path, no PDisk write yet.
    //   3. Complete A's PDisk write → A replies.
    //   4. Wakeup fires → combined write (header + B data) arrives.
    //      Verify no reply for B yet (write not complete).
    //   5. Complete the combined write → B's reply arrives.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteReplyDeferredUntilHeaderWritten) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(43, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 82, 1);

        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};

        // ── Step 1: send write A — keep PDisk write pending ───────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // ── Step 2: send write B while A is in-flight (batch path) ───────────
        // NO immediate PDisk write for B — data is accumulated internally.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: wakeup fires → combined write arrives ─────────────────────
        // Header sector + B's data sector = 2 * BlockSize.
        auto rawB_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_combined->Get()->Data.size(), 2 * BlockSize,
            "Batch combined write must contain header + data sectors (2 * BlockSize)");

        // Verify the sentinel is still pending (no spurious reply for B before the write completes).
        {
            TActorId sentinel = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
            ctx.Runtime.Send(new IEventHandle(sentinel, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
            auto ev = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, sentinel});
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinel,
                "B must not reply before the combined write completes");
        }

        // ── Step 5: complete the combined write → B's reply arrives ──────────
        ctx.SendPDiskResponse(disk, *rawB_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: the combined write issued by ProcessPersistentBufferBatchWrite
    // contains correct TPersistentBufferLsnRecordHeader metadata for the
    // batched record (TabletId, Generation, VChunkIndex, OffsetInBytes, Size, Lsn).
    //
    // The header is in the FIRST BlockSize bytes of the combined write.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path, no PDisk write yet.
    //   3. Complete A's PDisk write → A replies.
    //   4. Wakeup fires → combined write (header + B data) arrives.
    //   5. Parse the header bytes (first BlockSize bytes) and verify
    //      TPersistentBufferLsnRecordHeader fields match what was sent.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteHeaderSectorContainsCorrectMetadata) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(44, 1);

        const ui64 tabletId = 999;
        const ui32 generation = 7;
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, tabletId, generation);

        const ui64 vchunkIndex = 3;
        const ui32 offsetInBytes = 0;
        const ui32 size = BlockSize;
        const ui64 lsnB = 42;

        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{vchunkIndex, offsetInBytes, size};

        // ── Step 1: send write A — keep PDisk write pending ───────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // ── Step 2: send write B while A is in-flight (batch path) ───────────
        // NO immediate PDisk write for B.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: wakeup fires → combined write arrives ─────────────────────
        // Header sector + B's data sector = 2 * BlockSize.
        auto rawB_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_combined->Get()->Data.size(), 2 * BlockSize,
            "Batch combined write must contain header + data sectors (2 * BlockSize)");

        // ── Step 5: parse and verify TPersistentBufferLsnRecordHeader fields ──
        // The header is in the FIRST BlockSize bytes of the combined write.
        {
            TString rawData = rawB_combined->Get()->Data.ConvertToString();
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(rawData.data());

            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 1u,
                "BatchSize must be 1 for a single batched record");

            const auto* recHdr = reinterpret_cast<const NDDisk::TPersistentBufferLsnRecordHeader*>(
                rawData.data() + sizeof(NDDisk::TPersistentBufferHeader));

            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->TabletId, tabletId,
                "LsnRecordHeader.TabletId must match the write request");
            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->Generation, generation,
                "LsnRecordHeader.Generation must match the write request");
            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->VChunkIndex, vchunkIndex,
                "LsnRecordHeader.VChunkIndex must match the selector");
            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->OffsetInBytes, offsetInBytes,
                "LsnRecordHeader.OffsetInBytes must match the selector");
            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->Size, size,
                "LsnRecordHeader.Size must match the selector");
            UNIT_ASSERT_VALUES_EQUAL_C(recHdr->Lsn, lsnB,
                "LsnRecordHeader.Lsn must match the write request");
        }

        // Complete the combined write so the actor can clean up.
        ctx.SendPDiskResponse(disk, *rawB_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: when two batch records share a single batch, both records
    // receive their replies after the single combined write completes.
    //
    // NEW behavior: neither B nor C triggers any immediate PDisk write.
    // At wakeup time, ONE combined write is issued:
    //   header sector + B's data sector + C's data sector = 3 * BlockSize.
    // Both B and C reply after this single write completes.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path (first batch record).
    //      No immediate PDisk write for B.
    //   3. Send write C while B's batch cookie is active → batch path (second).
    //      No immediate PDisk write for C.
    //   4. Complete A's PDisk write → A replies.
    //   5. Wakeup fires → single combined write: header + B data + C data (3 * BlockSize).
    //   6. Complete combined write → both B and C reply with OK.
    //   7. Verify both B and C are readable.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteTwoRecordsShareSingleHeaderWrite) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(45, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 83, 1);

        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const TString payloadC = MakeData('C', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};
        const NDDisk::TBlockSelector selectorC{3, 0, BlockSize};
        const ui64 lsnB = 10;
        const ui64 lsnC = 20;

        // ── Step 1: send write A — keep PDisk write pending ───────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // ── Step 2: send write B while A is in-flight (first batch record) ────
        // No immediate PDisk write for B.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // ── Step 3: send write C while batch cookie is active (second record) ─
        // No immediate PDisk write for C.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // ── Step 4: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: wakeup fires → single combined write ──────────────────────
        // B and C share the same batch inflight → ONE write: header + B data + C data = 3 * BlockSize.
        auto rawBC_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_combined->Get()->Data.size(), 3 * BlockSize,
            "Batch combined write must contain header + B data + C data (3 sectors total)");

        // Verify the first sector (header) has the PB signature.
        {
            TString rawData = rawBC_combined->Get()->Data.ConvertToString();
            UNIT_ASSERT_C(
                memcmp(rawData.data(),
                    NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature,
                    sizeof(NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature)) == 0,
                "First sector of combined write must start with PersistentBufferHeaderSignature");
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(rawData.data());
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u,
                "BatchSize in header must equal 2 for both batch records B and C");
        }

        // ── Step 6: complete combined write → both B and C reply ─────────────
        ctx.SendPDiskResponse(disk, *rawBC_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        // Both B and C share the same inflight → both replies arrive.
        auto writeResult1 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult1, TReplyStatus::OK);
        auto writeResult2 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult2, TReplyStatus::OK);

        // ── Step 7: verify both B and C are readable ──────────────────────────
        auto readB = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds, selectorB, lsnB, 1, {true}));
        AssertStatus(readB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readB->Get()->GetPayload(0).ConvertToString(), payloadB);

        auto readC = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds, selectorC, lsnC, 1, {true}));
        AssertStatus(readC, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readC->Get()->GetPayload(0).ConvertToString(), payloadC);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: when the combined write fails, the batch write must reply
    // with an error and the allocated sectors must be freed.
    //
    // NEW behavior: B does NOT trigger any immediate PDisk write.
    // At wakeup time, a COMBINED write (header + B data) is issued.
    // This combined write is intercepted and injected with a failure.
    //
    // Injection strategy: intercept TEvWritePersistentBufferPart for the
    // combined write (IsErase=false) and replace it with a failed version.
    //
    // Sector count: B allocates 2 sectors (1 header + 1 data).
    // After failure, both must be freed → freeAfterFail == freeAfterA + 2.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteHeaderWriteFailureReturnsError) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(47, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 84, 1);

        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};

        // ── Send write A — keep PDisk write pending ───────────────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorA, 1, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // Capture free sectors BEFORE B is sent (A's sectors already allocated,
        // B's batch pre-allocation has not happened yet).
        // After a failed batch write, ALL pre-allocated batch sectors are freed,
        // restoring the pool to this baseline.
        const ui32 freeBeforeB = GetPBFreeSectors(ctx, disk);

        // ── Send write B while A is in-flight (batch path) ───────────────────
        // No immediate PDisk write for B. ProcessPersistentBufferBatchWriteData
        // pre-allocates MaxLsnsPerPack * MaxSectorsPerPackBufferRecord + 1 sectors.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }

        // Complete A's PDisk write → A replies.
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // Wakeup fires → combined write (header + B data) arrives.
        // Install a filter BEFORE completing the combined write to intercept
        // the TEvWritePersistentBufferPart completion and inject failure.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted &&
                    ev->GetTypeRewrite() == NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::EventType) {
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>*>(ev.get());
                if (!orig->Get()->IsErase) {
                    // This is the combined write completion — inject failure.
                    intercepted = true;
                    auto failed = std::make_unique<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>(
                        orig->Get()->InflightCookie,
                        orig->Get()->PartCookie,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                        "injected combined write failure");
                    ev.reset(new IEventHandle(ev->Recipient, ev->Sender, failed.release(), 0, ev->Cookie));
                }
            }
            return true;
        };

        // Wait for the combined write request (wakeup fires after A completes).
        auto rawB_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_combined->Get()->Data.size(), 2 * BlockSize,
            "Batch combined write must contain header + data sectors (2 * BlockSize)");

        // Acknowledge the raw combined write with OK so the actor stays alive.
        ctx.SendPDiskResponse(disk, *rawB_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // B must receive an error reply.
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        ctx.Runtime.FilterFunction = {};

        UNIT_ASSERT_C(intercepted, "Filter must have fired for the combined write");
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(writeResultB->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "Batch write must fail when the combined write fails");

        // B must NOT appear in the list.
        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 1,
            "Only record A must remain after B's combined write failed");

        // All sectors pre-allocated for the batch (MaxLsnsPerPack * MaxSectorsPerPackBufferRecord + 1)
        // must be freed after failure, restoring the pool to its state before B was sent.
        const ui32 freeAfterFail = GetPBFreeSectors(ctx, disk);
        UNIT_ASSERT_VALUES_EQUAL_C(freeAfterFail, freeBeforeB,
            "All sectors pre-allocated for the failed batch write must be freed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: batch-written records are correctly restored after a restart.
    //
    // Write sequence:
    //   A  — non-batch (keeps PDisk write in-flight so B enters batch path)
    //   B  — batch (data accumulated while A in-flight, no immediate write)
    //   C  — batch (data accumulated while B's batch cookie is active, same batch)
    //
    // NEW behavior: B and C generate NO immediate PDisk writes.
    // At wakeup time, ONE combined write is issued:
    //   header sector + B's data sector + C's data sector = 3 * BlockSize.
    //
    // B and C share ONE header sector (BatchSize=2).  After restart, all three
    // records must be listed and both B and C must be readable with their original
    // payloads.
    //
    // Covers: RestorePersistentBufferChunk BatchSize loop.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteRecordRestoredAfterRestart) {
        TTestContext ctx;

        const TDiskHandle disk1 = ctx.CreateDDisk(50, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 90, 1);

        const ui64 lsnA = 1;
        const ui64 lsnB = 77;
        const ui64 lsnC = 88;
        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const TString payloadC = MakeData('C', BlockSize);
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{2, 0, BlockSize};
        const NDDisk::TBlockSelector selectorC{3, 0, BlockSize};

        // Accumulate all raw PDisk writes into per-chunk buffers so we can feed them
        // back to disk2 during restore.
        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize,
                "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        // ── Step 1: send A, keep its PDisk write pending ──────────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorA, lsnA, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        // Capture disk1's actual actor ID for the filter in Phase 2.
        const TActorId disk1ActorId = rawA->Sender;
        captureWrite(rawA);

        // ── Step 2: send B while A is in-flight → B enters batch path ────────
        // No immediate PDisk write for B.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // ── Step 3: send C while B's batch cookie is active (same batch) ─────
        // No immediate PDisk write for C.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // ── Step 4: complete A → A replies ───────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: wakeup fires → combined write for B and C ─────────────────
        // ONE combined write: header sector + B data + C data = 3 * BlockSize.
        auto rawBC_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_combined->Get()->Data.size(), 3 * BlockSize,
            "Combined write must contain header + B data + C data (3 sectors)");
        captureWrite(rawBC_combined);

        // Verify BatchSize == 2 in the written header (first sector of the combined write).
        const ui32 headerChunkIdx = rawBC_combined->Get()->ChunkIdx;
        const ui32 headerOffset   = rawBC_combined->Get()->Offset;
        const ui64 actualUniqueId = [&]() -> ui64 {
            const TString& buf = chunkBufs[headerChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(
                buf.data() + headerOffset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u,
                "Header must record BatchSize=2 for B and C");
            return hdr->PersistentBufferUniqueId;
        }();
        UNIT_ASSERT_C(actualUniqueId != 0, "PersistentBufferUniqueId must not be zero");

        // ── Step 6: complete combined write → B and C both reply ─────────────
        ctx.SendPDiskResponse(disk1, *rawBC_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
        auto writeResultC = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultC, TReplyStatus::OK);

        // Collect the chunk IDs owned by instance 1.
        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }

        // ── Phase 2: restart with instance 2 (same PDiskId, same UniqueId) ───
        std::optional<TActorId> disk2PdiskEdge;

        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            // Drop all PDisk-bound events from disk1's actor.
            if (ev->Sender == disk1ActorId) {
                if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                    ctx.Runtime.Send(new IEventHandle(ev->Sender, disk1.PDiskEdge,
                        new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                        0, ev->Cookie), NodeId);
                }
                return false;
            }
            // Auto-respond to TEvChunkReadRaw from disk2 with the captured chunk slice.
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

        // Pass (actualUniqueId - 1) as oldUniqueId so disk2 uses
        // (actualUniqueId - 1 + 1) = actualUniqueId — matching the written checksums.
        // Must use the same PDiskId (50) as disk1 because PDiskId is part of the checksum.
        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            50, 1,
            pbChunkIds,
            /*oldUniqueId=*/actualUniqueId - 1,
            chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;

        // ── Phase 3: verify all three records are restored ────────────────────
        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 90, 1);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 3,
            "Records A, B and C must all be restored after restart");

        // Verify B's data.
        auto readB = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds2, selectorB, lsnB, 1, {true}));
        AssertStatus(readB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readB->Get()->GetPayload(0).ConvertToString(), payloadB,
            "Restored batch record B must have the original payload");

        // Verify C's data — this exercises the second iteration of the BatchSize loop
        // and the pos-advance fix (pos += (Sectors.size()-1) * sizeof(SectorInfo)).
        auto readC = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds2, selectorC, lsnC, 1, {true}));
        AssertStatus(readC, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readC->Get()->GetPayload(0).ConvertToString(), payloadC,
            "Restored batch record C must have the original payload");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7: batch-written records from DIFFERENT tablets are correctly
    // restored after a restart.
    //
    // Write sequence:
    //   A  — non-batch, tablet 91 gen 1  (keeps PDisk write in-flight so B/C
    //         enter the batch path)
    //   B  — batch, tablet 92 gen 1      (accumulated while A in-flight)
    //   C  — batch, tablet 93 gen 2      (accumulated while B's batch cookie active,
    //         same batch as B, BatchSize=2)
    //
    // NEW behavior: B and C generate NO immediate PDisk writes.
    // At wakeup time, ONE combined write is issued:
    //   header sector + B's data sector + C's data sector = 3 * BlockSize.
    //
    // After restart all three records must be listed and both B and C must be
    // readable with their original payloads via their respective credentials.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(BatchWriteMultiTabletRecordsRestoredAfterRestart) {
        TTestContext ctx;

        const TDiskHandle disk1 = ctx.CreateDDisk(55, 1);

        // Three distinct tablets.
        NDDisk::TQueryCredentials credsA = Connect(ctx, disk1.PBServiceId, 91, 1);
        NDDisk::TQueryCredentials credsB = Connect(ctx, disk1.PBServiceId, 92, 1);
        NDDisk::TQueryCredentials credsC = Connect(ctx, disk1.PBServiceId, 93, 2);

        const ui64 lsnA = 1;
        const ui64 lsnB = 55;
        const ui64 lsnC = 66;
        const TString payloadA = MakeData('A', BlockSize);
        const TString payloadB = MakeData('B', BlockSize);
        const TString payloadC = MakeData('C', BlockSize);
        // Each tablet uses its own VChunkIndex namespace, so they can all use
        // VChunkIndex=1 without collision.
        const NDDisk::TBlockSelector selectorA{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorB{1, 0, BlockSize};
        const NDDisk::TBlockSelector selectorC{1, 0, BlockSize};

        // Accumulate all raw PDisk writes into per-chunk buffers.
        std::unordered_map<ui32, TString> chunkBufs;
        auto captureWrite = [&](const std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>& raw) {
            const ui32 chunkIdx = raw->Get()->ChunkIdx;
            const ui32 offset   = raw->Get()->Offset;
            TString written = raw->Get()->Data.ConvertToString();
            if (chunkBufs.find(chunkIdx) == chunkBufs.end()) {
                chunkBufs[chunkIdx] = TString(TTestContext::ChunkSize, '\0');
            }
            UNIT_ASSERT_C(offset + written.size() <= TTestContext::ChunkSize,
                "Write exceeds chunk size");
            memcpy(chunkBufs[chunkIdx].Detach() + offset, written.data(), written.size());
        };

        // ── Step 1: send A (tablet 91), keep its PDisk write pending ─────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                credsA, selectorA, lsnA, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadA));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawA = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        const TActorId disk1ActorId = rawA->Sender;
        captureWrite(rawA);

        // ── Step 2: send B (tablet 92) while A is in-flight → batch path ─────
        // No immediate PDisk write for B.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                credsB, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // ── Step 3: send C (tablet 93 gen 2) while B's batch cookie is active ─
        // No immediate PDisk write for C.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                credsC, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }

        // ── Step 4: complete A → A replies ───────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: wakeup fires → combined write for B and C ─────────────────
        // ONE combined write: header sector + B data + C data = 3 * BlockSize.
        auto rawBC_combined = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_combined->Get()->Data.size(), 3 * BlockSize,
            "Combined write must contain header + B data + C data (3 sectors)");
        captureWrite(rawBC_combined);

        // Verify BatchSize == 2 and extract PersistentBufferUniqueId.
        //
        // On-disk layout inside the header sector (interleaved per record):
        //   TPersistentBufferHeader
        //   [record 0] TPersistentBufferLsnRecordHeader
        //              (Sectors.size() - 1) × TPersistentBufferSectorInfo
        //   [record 1] TPersistentBufferLsnRecordHeader
        //              (Sectors.size() - 1) × TPersistentBufferSectorInfo
        //   ...
        // Each record here has exactly 1 data sector, so Sectors.size() == 2
        // (header sector + 1 data sector), meaning (Sectors.size()-1) == 1
        // TPersistentBufferSectorInfo entry follows each LsnRecordHeader.
        const ui32 headerChunkIdx = rawBC_combined->Get()->ChunkIdx;
        const ui32 headerOffset   = rawBC_combined->Get()->Offset;
        const ui64 actualUniqueId = [&]() -> ui64 {
            const TString& buf = chunkBufs[headerChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(
                buf.data() + headerOffset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u,
                "Header must record BatchSize=2 for B and C");

            // Record B: LsnRecordHeader immediately after TPersistentBufferHeader.
            const char* pos = buf.data() + headerOffset + sizeof(NDDisk::TPersistentBufferHeader);
            const auto* recB = reinterpret_cast<const NDDisk::TPersistentBufferLsnRecordHeader*>(pos);
            UNIT_ASSERT_VALUES_EQUAL_C(recB->TabletId, credsB.TabletId,
                "First batched record must belong to tablet B");
            UNIT_ASSERT_VALUES_EQUAL_C(recB->Generation, credsB.Generation,
                "First batched record generation must match tablet B");

            // Advance past recB's LsnRecordHeader and its 1 TPersistentBufferSectorInfo
            // (Sectors.size()-1 == 1 for a single-sector data write).
            pos += sizeof(NDDisk::TPersistentBufferLsnRecordHeader);
            pos += sizeof(NDDisk::TPersistentBufferSectorInfo); // (Sectors.size()-1) entries

            // Record C: immediately follows.
            const auto* recC = reinterpret_cast<const NDDisk::TPersistentBufferLsnRecordHeader*>(pos);
            UNIT_ASSERT_VALUES_EQUAL_C(recC->TabletId, credsC.TabletId,
                "Second batched record must belong to tablet C");
            UNIT_ASSERT_VALUES_EQUAL_C(recC->Generation, credsC.Generation,
                "Second batched record generation must match tablet C");

            return hdr->PersistentBufferUniqueId;
        }();
        UNIT_ASSERT_C(actualUniqueId != 0, "PersistentBufferUniqueId must not be zero");

        // ── Step 6: complete combined write → B and C both reply ─────────────
        ctx.SendPDiskResponse(disk1, *rawBC_combined, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
        auto writeResultC = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultC, TReplyStatus::OK);

        // Collect the chunk IDs owned by instance 1.
        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }

        // ── Phase 2: restart with instance 2 (same PDiskId=55) ───────────────
        std::optional<TActorId> disk2PdiskEdge;

        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            // Drop all PDisk-bound events from disk1's actor.
            if (ev->Sender == disk1ActorId) {
                if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                    ctx.Runtime.Send(new IEventHandle(ev->Sender, disk1.PDiskEdge,
                        new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                        0, ev->Cookie), NodeId);
                }
                return false;
            }
            // Auto-respond to TEvChunkReadRaw from disk2 with the captured chunk slice.
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

        // Pass (actualUniqueId - 1) as oldUniqueId so disk2 uses
        // (actualUniqueId - 1 + 1) = actualUniqueId — matching the written checksums.
        // Must use the same PDiskId (55) as disk1 because PDiskId is part of the checksum.
        const TDiskHandle disk2 = ctx.CreateDDiskWithRestoredChunkData(
            55, 1,
            pbChunkIds,
            /*oldUniqueId=*/actualUniqueId - 1,
            chunkBufs);
        disk2PdiskEdge = disk2.PDiskEdge;

        // ── Phase 3: verify all three records are restored ────────────────────
        // Connect with each tablet's credentials separately.
        NDDisk::TQueryCredentials creds2A = Connect(ctx, disk2.PBServiceId, 91, 1);
        NDDisk::TQueryCredentials creds2B = Connect(ctx, disk2.PBServiceId, 92, 1);
        NDDisk::TQueryCredentials creds2C = Connect(ctx, disk2.PBServiceId, 93, 2);

        // List from tablet A's perspective — must see exactly A's record.
        auto listA = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2A));
        AssertStatus(listA, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listA->Get()->Record.RecordsSize(), 1,
            "Tablet A must have exactly 1 restored record");

        // List from tablet B's perspective — must see exactly B's record.
        auto listB = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2B));
        AssertStatus(listB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listB->Get()->Record.RecordsSize(), 1,
            "Tablet B must have exactly 1 restored record");

        // List from tablet C's perspective — must see exactly C's record.
        auto listC = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2C));
        AssertStatus(listC, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listC->Get()->Record.RecordsSize(), 1,
            "Tablet C must have exactly 1 restored record");

        // Verify B's payload.
        // The 4th argument to TEvReadPersistentBuffer is the record's generation
        // (used as the key in PersistentBuffers map: {TabletId, generation}).
        auto readB = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds2B, selectorB, lsnB, credsB.Generation, {true}));
        AssertStatus(readB, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readB->Get()->GetPayload(0).ConvertToString(), payloadB,
            "Restored batch record B (tablet 92) must have the original payload");

        // Verify C's payload — exercises the second iteration of the BatchSize loop
        // and the pos-advance fix for multi-sector records.
        // C was written with generation=2, so the lookup key is {93, 2}.
        auto readC = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId,
            new NDDisk::TEvReadPersistentBuffer(creds2C, selectorC, lsnC, credsC.Generation, {true}));
        AssertStatus(readC, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(readC->Get()->GetPayload(0).ConvertToString(), payloadC,
            "Restored batch record C (tablet 93 gen 2) must have the original payload");
    }

} // Y_UNIT_TEST_SUITE(TDDiskActorBatchWriteTest)

} // namespace NKikimr
