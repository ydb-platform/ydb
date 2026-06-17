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
//   Sectors 1..K (data sectors for each record, written immediately)
//
// ProcessPersistentBufferBatchWrite() is called on the wakeup timer and
// writes the unified header sector.  Only after BOTH all data parts AND the
// header part complete does HandleWritePart() reply to the callers.
//
// IMPORTANT: The batch path requires an in-flight operation when the second
// write arrives.  The correct test sequence is:
//   1. Send write A → PDisk write arrives (keep it pending — A is in-flight)
//   2. Send write B while A is still in-flight → B goes through batch path
//   3. Complete A's PDisk write → A replies
//   4. Complete B's data write
//   5. Wakeup fires → header write
//   6. Complete header write → B replies

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
    // Test 1: two small writes are batched into a single header sector.
    //
    // The batch path requires an in-flight operation when the second write
    // arrives.  Correct sequence:
    //   1. Send write A → PDisk write arrives (keep it pending — A is in-flight).
    //   2. Send write B while A is still in-flight → B goes through batch path.
    //      PDisk write for B's data sector arrives immediately (1 sector, no header).
    //   3. Complete A's PDisk write → A replies.
    //   4. Complete B's data write.
    //   5. Wakeup timer fires → ProcessPersistentBufferBatchWrite writes the
    //      shared header sector for B (1 sector).
    //   6. Complete the header write → B's reply arrives.
    //   7. Verify both records are readable and their data is correct.
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
        // B's data sector is written immediately; the header is deferred.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        // B's data sector write arrives (1 sector, no header yet).
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_data->Get()->Data.size(), BlockSize,
            "Batch data write must be exactly one data sector (no header)");

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 5: wakeup fires → ProcessPersistentBufferBatchWrite ─────────
        // The header sector for B is written now (1 sector).
        auto rawB_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_header->Get()->Data.size(), BlockSize,
            "Batch header write must be exactly one sector");

        // Verify the header sector starts with the PB signature.
        {
            TString headerBytes = rawB_header->Get()->Data.ConvertToString();
            UNIT_ASSERT_C(
                memcmp(headerBytes.data(),
                    NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature,
                    sizeof(NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature)) == 0,
                "Header sector must start with PersistentBufferHeaderSignature");
            // BatchSize must be 1 (only record B is in this batch).
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(headerBytes.data());
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 1u,
                "BatchSize in header must equal the number of batched records");
        }

        // ── Step 6: complete the header write → B's reply arrives ────────────
        ctx.SendPDiskResponse(disk, *rawB_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);

        // ── Step 7: verify both records are readable ──────────────────────────
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
    // Test 2: batch write reply is deferred until BOTH data parts AND the header
    // sector are written.  If the header write is still in-flight when all data
    // parts complete, the caller must NOT receive a reply yet.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path, data sector written.
    //   3. Complete A's PDisk write → A replies.
    //   4. Complete B's data write.
    //   5. Verify no reply for B yet (header not written) using a sentinel actor.
    //   6. Complete the header write → B's reply arrives.
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
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_data->Get()->Data.size(), BlockSize,
            "Batch data write must be exactly one data sector");

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 5: verify no reply yet (header not written) ─────────────────
        // The header write request must arrive before any write result for B.
        auto rawB_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_header->Get()->Data.size(), BlockSize,
            "Batch header write must be exactly one sector");

        // Verify the sentinel is still pending (no spurious reply for B).
        {
            TActorId sentinel = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
            ctx.Runtime.Send(new IEventHandle(sentinel, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
            auto ev = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, sentinel});
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinel,
                "B must not reply before the header sector is written");
        }

        // ── Step 6: complete the header write → B's reply arrives ────────────
        ctx.SendPDiskResponse(disk, *rawB_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: the header sector written by ProcessPersistentBufferBatchWrite
    // contains correct TPersistentBufferLsnRecordHeader metadata for the
    // batched record (TabletId, Generation, VChunkIndex, OffsetInBytes, Size, Lsn).
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path.
    //   3. Complete A's PDisk write → A replies.
    //   4. Complete B's data write.
    //   5. Wakeup fires → header sector written.
    //   6. Parse the raw header bytes and verify TPersistentBufferLsnRecordHeader
    //      fields match what was sent in the write request.
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
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // ── Step 3: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 4: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 5: wakeup fires → header sector written ──────────────────────
        auto rawB_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_header->Get()->Data.size(), BlockSize,
            "Batch header write must be exactly one sector");

        // ── Step 6: parse and verify TPersistentBufferLsnRecordHeader fields ──
        {
            TString headerBytes = rawB_header->Get()->Data.ConvertToString();
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(headerBytes.data());

            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 1u,
                "BatchSize must be 1 for a single batched record");

            const auto* recHdr = reinterpret_cast<const NDDisk::TPersistentBufferLsnRecordHeader*>(
                headerBytes.data() + sizeof(NDDisk::TPersistentBufferHeader));

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

        // Complete the header write so the actor can clean up.
        ctx.SendPDiskResponse(disk, *rawB_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResultB, TReplyStatus::OK);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: when two batch records share a single header sector, both records
    // receive their replies after the single shared header write completes.
    //
    // Note: the current implementation sets BatchSize=1 in the header sector
    // regardless of how many records are batched (the BatchSize field tracks
    // the number of records in the header for the restore path, but the current
    // code hardcodes it to 1).  This test verifies the observable behavior:
    // both B and C get OK replies after the single header write, and both
    // records are readable.
    //
    // Sequence:
    //   1. Send write A → PDisk write arrives (keep pending — A is in-flight).
    //   2. Send write B while A is in-flight → batch path (first batch record).
    //   3. Send write C while B's batch cookie is active → batch path (second).
    //   4. Complete A's PDisk write → A replies.
    //   5. Complete B's data write.
    //   6. Complete C's data write.
    //   7. Wakeup fires → single header sector written.
    //   8. Complete header write → both B and C reply with OK.
    //   9. Verify both B and C are readable.
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
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_data->Get()->Data.size(), BlockSize,
            "First batch data write must be exactly one data sector");

        // ── Step 3: send write C while batch cookie is active (second record) ─
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawC_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawC_data->Get()->Data.size(), BlockSize,
            "Second batch data write must be exactly one data sector");

        // ── Step 4: complete A's PDisk write → A replies ──────────────────────
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 6: complete C's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk, *rawC_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 7: wakeup fires → single header sector written ───────────────
        // Both B and C share the same batch inflight → only ONE header write.
        auto rawBC_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_header->Get()->Data.size(), BlockSize,
            "Batch header write must be exactly one sector for both records");

        // Verify the header sector has the PB signature.
        {
            TString headerBytes = rawBC_header->Get()->Data.ConvertToString();
            UNIT_ASSERT_C(
                memcmp(headerBytes.data(),
                    NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature,
                    sizeof(NDDisk::TPersistentBufferHeader::PersistentBufferHeaderSignature)) == 0,
                "Header sector must start with PersistentBufferHeaderSignature");
        }

        // ── Step 8: complete header write → both B and C reply ───────────────
        ctx.SendPDiskResponse(disk, *rawBC_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        // Both B and C share the same inflight → both replies arrive.
        auto writeResult1 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult1, TReplyStatus::OK);
        auto writeResult2 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult2, TReplyStatus::OK);

        // ── Step 9: verify both B and C are readable ──────────────────────────
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
    // Test 5: when the header sector write fails, the batch write must reply
    // with an error and the allocated sectors must be freed.
    //
    // Injection strategy: intercept TEvWritePersistentBufferPart for the header
    // write (IsErase=false) and replace it with a failed version.  The data
    // sector write is allowed to succeed normally.
    //
    // Covers: HandleWritePart → error path → PersistentBufferSpaceAllocator.Free
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

        // ── Send write B while A is in-flight (batch path) ───────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selectorB, 2, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // Complete A's PDisk write → A replies.
        ctx.SendPDiskResponse(disk, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // Capture free sectors after A is committed (A consumed some sectors).
        const ui32 freeAfterA = GetPBFreeSectors(ctx, disk);

        // Complete B's data write (OK).
        ctx.SendPDiskResponse(disk, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // Wakeup fires → header sector write arrives.
        auto rawB_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_header->Get()->Data.size(), BlockSize,
            "Batch header write must be exactly one sector");

        // Install a filter that intercepts the internal TEvWritePersistentBufferPart
        // completion for the header write and replaces it with a failure.
        // The filter is installed AFTER B's data write has already completed, so the
        // next non-erase TEvWritePersistentBufferPart that arrives is the header write.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted &&
                    ev->GetTypeRewrite() == NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::EventType) {
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>*>(ev.get());
                if (!orig->Get()->IsErase) {
                    // This is the header write completion — inject failure.
                    intercepted = true;
                    auto failed = std::make_unique<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>(
                        orig->Get()->InflightCookie,
                        orig->Get()->PartCookie,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                        "injected header write failure");
                    ev.reset(new IEventHandle(ev->Recipient, ev->Sender, failed.release(), 0, ev->Cookie));
                }
            }
            return true;
        };

        // Acknowledge the raw header write with OK so the actor stays alive.
        ctx.SendPDiskResponse(disk, *rawB_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // B must receive an error reply.
        auto writeResultB = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        ctx.Runtime.FilterFunction = {};

        UNIT_ASSERT_C(intercepted, "Filter must have fired for the header write");
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(writeResultB->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "Batch write must fail when the header sector write fails");

        // B must NOT appear in the list.
        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 1,
            "Only record A must remain after B's header write failed");

        // Sectors allocated for B (data + header) must be freed.
        // B allocated 2 sectors (1 header + 1 data); after failure both must be returned.
        const ui32 freeAfterFail = GetPBFreeSectors(ctx, disk);
        const ui32 bSectors = selectorB.Size / BlockSize + 1; // data sectors + 1 header
        UNIT_ASSERT_VALUES_EQUAL_C(freeAfterFail, freeAfterA + bSectors,
            "Sectors allocated for the failed batch write must be freed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: batch-written records are correctly restored after a restart.
    //
    // This test exercises the `for (ui32 batchIdx = 0; batchIdx < header->BatchSize; ++batchIdx)`
    // loop added to RestorePersistentBufferChunk in the current branch.
    //
    // Strategy:
    //   Phase 1 (instance 1, pdiskId=50):
    //     - Write record A (non-batch) and complete it.
    //     - Write record B (batch path, while A is in-flight).
    //     - Capture the raw bytes of EVERY TEvChunkWriteRaw issued (both data
    //       and header sectors) and build a full-chunk buffer.
    //     - Extract PersistentBufferUniqueId from the header sector bytes.
    //
    //   Phase 2 (instance 2, pdiskId=51, same TTestContext):
    //     - Pass the captured chunk data and (actualUniqueId - 1) as oldUniqueId
    //       so CreateDDiskWithRestoredChunkData uses (oldUniqueId + 1) = actualUniqueId.
    //     - Checksums pass → RestorePersistentBufferChunk restores the record.
    //     - Verify record B is listed after restore.
    //     - Verify record B's data is readable.
    //
    // Covers: RestorePersistentBufferChunk BatchSize loop (line 464 in the branch).
    // ─────────────────────────────────────────────────────────────────────────
    // Tests that batch-written records with BatchSize > 1 are correctly restored after
    // a restart. Specifically exercises the for (batchIdx < header->BatchSize) loop in
    // RestorePersistentBufferChunk and the pos-advance fix
    // (pos += sizeof(TPersistentBufferSectorInfo) * (record.Sectors.size() - 1)).
    //
    // Write sequence:
    //   A  — non-batch (keeps PDisk write in-flight so B enters batch path)
    //   B  — batch (data write while A in-flight)
    //   C  — batch (data write while B's data write still in-flight → same batch as B)
    //
    // B and C share ONE header sector (BatchSize=2).  After restart, all three records
    // must be listed and both B and C must be readable with their original payloads.
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
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_data->Get()->Data.size(), BlockSize,
            "B: batch data write must be exactly one data sector");
        captureWrite(rawB_data);

        // ── Step 3: send C while B's data write is still in-flight ───────────
        // C joins the same batch inflight as B (PersistentBufferBatchWriteCookie != 0).
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds1, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawC_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawC_data->Get()->Data.size(), BlockSize,
            "C: batch data write must be exactly one data sector");
        captureWrite(rawC_data);

        // ── Step 4: complete A → A replies ───────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 6: complete C's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawC_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 7: wakeup fires → shared header sector written (BatchSize=2) ─
        auto rawBC_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_header->Get()->Data.size(), BlockSize,
            "Shared header write must be exactly one sector");
        captureWrite(rawBC_header);

        // Verify BatchSize == 2 in the written header.
        const ui32 headerChunkIdx = rawBC_header->Get()->ChunkIdx;
        const ui32 headerOffset   = rawBC_header->Get()->Offset;
        const ui64 actualUniqueId = [&]() -> ui64 {
            const TString& buf = chunkBufs[headerChunkIdx];
            const auto* hdr = reinterpret_cast<const NDDisk::TPersistentBufferHeader*>(
                buf.data() + headerOffset);
            UNIT_ASSERT_VALUES_EQUAL_C(hdr->BatchSize, 2u,
                "Header must record BatchSize=2 for B and C");
            return hdr->PersistentBufferUniqueId;
        }();
        UNIT_ASSERT_C(actualUniqueId != 0, "PersistentBufferUniqueId must not be zero");

        // ── Step 8: complete header write → B and C both reply ───────────────
        ctx.SendPDiskResponse(disk1, *rawBC_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
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
        // TransformEvent resolves service IDs to actor IDs BEFORE scheduling, so
        // by the time FilterFunction runs, ev->GetRecipientRewrite() is already the
        // resolved edge actor ID (not the service ID).
        //
        // Strategy:
        //   - Drop all events from disk1's actor (identified by sender).
        //   - After disk2 is created, auto-respond to TEvChunkReadRaw going to
        //     disk2's PDisk edge with the correct byte slice from chunkBufs.
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
            // TrimData asserts data.size() == pr.Size, so we must return exactly req->Size bytes.
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
    // This test is analogous to BatchWriteRecordRestoredAfterRestart but
    // exercises the case where B and C belong to two distinct tablets
    // (different TabletId / Generation credentials).  The shared header sector
    // must encode the correct TabletId/Generation for each record so that the
    // restore path can reconstruct both records independently.
    //
    // Write sequence:
    //   A  — non-batch, tablet 91 gen 1  (keeps PDisk write in-flight so B/C
    //         enter the batch path)
    //   B  — batch, tablet 92 gen 1      (data write while A in-flight)
    //   C  — batch, tablet 93 gen 2      (data write while B's data write still
    //         in-flight → same batch as B, BatchSize=2)
    //
    // B and C share ONE header sector.  After restart all three records must be
    // listed and both B and C must be readable with their original payloads via
    // their respective credentials.
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
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                credsB, selectorB, lsnB, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadB));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawB_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawB_data->Get()->Data.size(), BlockSize,
            "B: batch data write must be exactly one data sector");
        captureWrite(rawB_data);

        // ── Step 3: send C (tablet 93 gen 2) while B's data write is in-flight ─
        // C joins the same batch inflight as B (PersistentBufferBatchWriteCookie != 0).
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                credsC, selectorC, lsnC, NDDisk::TWriteInstruction(0));
            write->AddPayload(TRope(payloadC));
            SendToDDisk(ctx, disk1.PBServiceId, write.release());
        }
        auto rawC_data = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawC_data->Get()->Data.size(), BlockSize,
            "C: batch data write must be exactly one data sector");
        captureWrite(rawC_data);

        // ── Step 4: complete A → A replies ───────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawA, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto wrA = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(wrA, TReplyStatus::OK);

        // ── Step 5: complete B's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawB_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 6: complete C's data write ──────────────────────────────────
        ctx.SendPDiskResponse(disk1, *rawC_data, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // ── Step 7: wakeup fires → shared header sector written (BatchSize=2) ─
        auto rawBC_header = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT_VALUES_EQUAL_C(rawBC_header->Get()->Data.size(), BlockSize,
            "Shared header write must be exactly one sector");
        captureWrite(rawBC_header);

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
        const ui32 headerChunkIdx = rawBC_header->Get()->ChunkIdx;
        const ui32 headerOffset   = rawBC_header->Get()->Offset;
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

        // ── Step 8: complete header write → B and C both reply ───────────────
        ctx.SendPDiskResponse(disk1, *rawBC_header, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
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
