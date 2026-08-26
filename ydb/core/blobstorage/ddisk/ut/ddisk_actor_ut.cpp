#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor.h>
#include <ydb/core/blobstorage/ddisk/ddisk_checksums.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <initializer_list>
#include <map>
#include <set>
#include <tuple>

namespace NKikimr {
namespace {

using NKikimrBlobStorage::NDDisk::TReplyStatus;

constexpr ui32 NodeId = 1;
constexpr ui32 BlockSize = 4096;
constexpr ui32 MinChunksReserved = 4;
constexpr ui32 PersistentBufferInitChunks = 4;

static_assert(NDDisk::NPrivate::THasSelectorField<NKikimrBlobStorage::NDDisk::TEvWrite>::value);

struct TDiskHandle {
    TActorId ServiceId;
    TActorId PBServiceId;
    TActorId PDiskEdge;
    ui32 PDiskId;
    ui32 SlotId;
    ui32 FirstChunkId;
    bool EnableChecksums = true;
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
    std::set<TActorId> PDiskEdges;
    std::set<TActorId> PDiskServiceIds;
    std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>> HeldBootstrapRefill;

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

    TDiskHandle CreateDDisk(ui32 pdiskId, ui32 slotId,
            std::optional<NDDisk::TPersistentBufferFormat> customFormat = std::nullopt,
            NDDisk::TDDiskConfig ddiskConfig = {}) {
        TDiskHandle disk = RegisterDDisk(pdiskId, slotId, customFormat, std::move(ddiskConfig));
        BootstrapDDisk(disk);
        return disk;
    }

    // Registers the actors without running the bootstrap protocol: tests that need a custom boot
    // sequence (e.g. recovery from starting points) drive the PDisk side themselves.
    TDiskHandle RegisterDDisk(ui32 pdiskId, ui32 slotId,
            std::optional<NDDisk::TPersistentBufferFormat> customFormat = std::nullopt,
            NDDisk::TDDiskConfig ddiskConfig = {}) {
        const bool enableChecksums = ddiskConfig.EnableChecksums;
        const TActorId pdiskEdge = Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        const TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);
        Runtime.RegisterService(pdiskServiceId, pdiskEdge);
        PDiskEdges.insert(pdiskEdge);
        PDiskServiceIds.insert(pdiskServiceId);

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
        NDDisk::TPersistentBufferFormat pbFormat = customFormat.value_or(
            NDDisk::TPersistentBufferFormat{256, 4, BlockSize * 128, 8, 5000, 512 * 1024});
        const TActorId ddiskActor = Runtime.Register(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), std::move(ddiskConfig), Counters),
            NodeId);
        const TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, slotId);
        const TActorId pbServiceId = MakeBlobStoragePersistentBufferId(NodeId, pdiskId, slotId);
        Runtime.RegisterService(ddiskServiceId, ddiskActor);

        return TDiskHandle{
            ddiskServiceId,
            pbServiceId,
            pdiskEdge,
            pdiskId,
            slotId,
            100000 + pdiskId * 1000,
            enableChecksums};
    }

    std::set<TActorId> ClientWaitEdges(std::initializer_list<TActorId> extra = {}) const {
        std::set<TActorId> edges = PDiskEdges;
        edges.insert(Edge);
        for (const TActorId& id : extra) {
            edges.insert(id);
        }
        return edges;
    }

    // Consume a PDisk-bound event that arrived while the test was waiting for a client
    // reply. CheckSpace is acked so PB occupancy polling cannot stall; everything else
    // is dropped unreplied (in-flight formatting / data I/O after Terminate or Broken).
    bool ConsumeUnsolicitedPDiskEvent(std::unique_ptr<IEventHandle>& raw) {
        if (!PDiskEdges.contains(raw->Recipient) && !PDiskServiceIds.contains(raw->Recipient)) {
            return false;
        }
        if (raw->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
            SendFromPDisk(Runtime, raw->Recipient, raw->Sender,
                new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0), raw->Cookie);
        }
        return true;
    }

    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequest(const TDiskHandle& disk) {
        return WaitPDiskRequests<TEvent>({disk.PDiskEdge});
    }

    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequests(const std::set<TActorId>& disks) {
        for (;;) {
            std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent(disks);
            if (TryAutoServeIntegrityTraffic<TEvent>(*raw)) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TEvent::EventType);
            return RecastEvent<TEvent>(std::move(raw));
        }
    }

    // For tests that inspect the integrity traffic itself and must see every PDisk event
    // except periodic TEvCheckSpace (PB occupancy polling), which is auto-acked.
    template<typename TEvent>
    std::unique_ptr<TEventHandle<TEvent>> WaitPDiskRequestNoAutoServe(const TDiskHandle& disk) {
        for (;;) {
            std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
            if (raw->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                SendFromPDisk(Runtime, raw->Recipient, raw->Sender,
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0), raw->Cookie);
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TEvent::EventType);
            return RecastEvent<TEvent>(std::move(raw));
        }
    }

    // Integrity metadata I/O (chunk header replicas and extent-format writes, recognized by their
    // magics) and reserve refills that a test script is not explicitly waiting for are
    // transparently acknowledged, so scripts keep seeing only the data traffic they were written
    // for. Combined allocation increments are *not* auto-served: they commit the data chunk and
    // gate the client reply.
    template<typename TExpectedEvent>
    bool TryAutoServeIntegrityTraffic(IEventHandle& raw) {
        if (raw.GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType) {
            const auto* write = raw.CastAsLocal<NPDisk::TEvChunkWriteRaw>();
            if (IsIntegrityMetadataWrite(*write)) {
                AutoServedIntegrityWriteChunks.push_back(write->ChunkIdx);
                SendFromPDisk(Runtime, raw.Recipient, raw.Sender,
                    new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""), raw.Cookie);
                return true;
            }
        } else if (raw.GetTypeRewrite() == NPDisk::TEvChunkReserve::EventType
                && TExpectedEvent::EventType != NPDisk::TEvChunkReserve::EventType) {
            const auto* reserve = raw.CastAsLocal<NPDisk::TEvChunkReserve>();
            auto reply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
            for (ui32 i = 0; i < reserve->SizeChunks; ++i) {
                reply->ChunkIds.push_back(NextAutoReserveChunkId++);
            }
            SendFromPDisk(Runtime, raw.Recipient, raw.Sender, reply.release(), raw.Cookie);
            return true;
        } else if (raw.GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType
                && TExpectedEvent::EventType != NPDisk::TEvCheckSpace::EventType) {
            SendFromPDisk(Runtime, raw.Recipient, raw.Sender,
                new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0), raw.Cookie);
            return true;
        }
        return false;
    }

    static bool IsIntegrityMetadataWrite(const NPDisk::TEvChunkWriteRaw& write) {
        auto it = write.Data.Begin();
        if (!it.Valid() || it.ContiguousSize() < sizeof(ui64)) {
            return false;
        }
        ui64 magic;
        memcpy(&magic, it.ContiguousData(), sizeof(magic));
        return magic == NDDisk::MagicIntegrityChunkHeader || magic == NDDisk::MagicIntegrityBlock;
    }

    static NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord ParseChunkMapLog(
            const NPDisk::TEvLog& log) {
        UNIT_ASSERT(log.Signature.GetUnmasked() == TLogSignature::SignatureDDiskChunkMap);
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
        UNIT_ASSERT(record.ParseFromArray(log.Data.data(), log.Data.size()));
        return record;
    }

    void ReplyLog(const TDiskHandle& disk, TEventHandle<NPDisk::TEvLog>& req) {
        auto r = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        r->Results.emplace_back(req.Get()->Lsn, req.Get()->Cookie);
        SendPDiskResponse(disk, req, r.release());
    }

    struct TAllocationTraffic {
        std::unique_ptr<TEventHandle<NPDisk::TEvLog>> Snapshot;
        std::unique_ptr<TEventHandle<NPDisk::TEvLog>> Increment;
        std::vector<std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>> DataWrites;
        std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>> Reserve;
    };

    // Formatting I/O, the data write and the combined increment may appear in any order.
    // Integrity metadata writes are auto-served (so the increment can be issued). Reserves are
    // auto-served unless holdReserve, in which case the first refill is captured unreplied.
    TAllocationTraffic CollectAllocationTraffic(const TDiskHandle& disk,
            bool expectSnapshot, ui32 expectedDataWrites, bool holdReserve = false) {
        TAllocationTraffic traffic;
        ui32 guard = 0;
        while ((expectSnapshot && !traffic.Snapshot) || !traffic.Increment
                || traffic.DataWrites.size() < expectedDataWrites) {
            UNIT_ASSERT_C(++guard < 200, "timed out collecting allocation PDisk traffic");
            std::unique_ptr<IEventHandle> raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
            const ui32 type = raw->GetTypeRewrite();
            if (type == NPDisk::TEvChunkWriteRaw::EventType) {
                auto write = RecastEvent<NPDisk::TEvChunkWriteRaw>(std::move(raw));
                if (IsIntegrityMetadataWrite(*write->Get())) {
                    AutoServedIntegrityWriteChunks.push_back(write->Get()->ChunkIdx);
                    SendPDiskResponse(disk, *write, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
                    continue;
                }
                traffic.DataWrites.push_back(std::move(write));
                continue;
            }
            if (type == NPDisk::TEvLog::EventType) {
                auto log = RecastEvent<NPDisk::TEvLog>(std::move(raw));
                const auto record = ParseChunkMapLog(*log->Get());
                if (record.HasSnapshot()) {
                    UNIT_ASSERT(expectSnapshot);
                    UNIT_ASSERT(!traffic.Snapshot);
                    ReplyLog(disk, *log);
                    traffic.Snapshot = std::move(log);
                    continue;
                }
                UNIT_ASSERT(record.HasIncrement());
                UNIT_ASSERT(!traffic.Increment);
                traffic.Increment = std::move(log);
                continue;
            }
            if (type == NPDisk::TEvChunkReserve::EventType) {
                auto reserve = RecastEvent<NPDisk::TEvChunkReserve>(std::move(raw));
                if (holdReserve) {
                    UNIT_ASSERT(!traffic.Reserve);
                    traffic.Reserve = std::move(reserve);
                    continue;
                }
                auto reply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
                for (ui32 i = 0; i < reserve->Get()->SizeChunks; ++i) {
                    reply->ChunkIds.push_back(NextAutoReserveChunkId++);
                }
                SendPDiskResponse(disk, *reserve, reply.release());
                continue;
            }
            if (type == NPDisk::TEvCheckSpace::EventType) {
                auto checkSpace = RecastEvent<NPDisk::TEvCheckSpace>(std::move(raw));
                SendPDiskResponse(disk, *checkSpace,
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0));
                --guard; // occupancy polling is not allocation traffic
                continue;
            }
            UNIT_ASSERT_C(false, "unexpected PDisk event type " << type);
        }
        return traffic;
    }

    // Fresh ids for auto-served reserve refills; far away from ids the tests assert on.
    ui32 NextAutoReserveChunkId = 900000;

    // Chunk ids of every auto-served integrity metadata write, so tests can check which chunks
    // were (re-)formatted behind their back.
    std::vector<ui32> AutoServedIntegrityWriteChunks;

    template<typename TRequestEvent>
    void SendPDiskResponse(const TDiskHandle& disk, const TEventHandle<TRequestEvent>& request, IEventBase* response) {
        SendFromPDisk(Runtime, disk.PDiskEdge, request.Sender, response, request.Cookie);
    }

    void BootstrapDDisk(const TDiskHandle& disk, ui32 chunkSize = ChunkSize,
            ui32 ddiskReserveChunks = MinChunksReserved,
            const NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord* chunkMapSnapshot = nullptr,
            ui64 chunkMapSnapshotLsn = 0,
            const std::vector<std::pair<
                NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord, ui64>>& replay = {},
            TVector<TChunkIdx>* bootReclaimedChunks = nullptr) {
        HeldBootstrapRefill.reset();
        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;

        auto init = WaitPDiskRequest<NPDisk::TEvYardInit>(disk);
        TVector<ui32> ownedChunks;
        auto initReply = std::make_unique<NPDisk::TEvYardInitResult>(
            NKikimrProto::OK,
            0, 0, 0, // seek/read/write speed
            BlockSize, BlockSize, BlockSize,
            chunkSize,
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
        format.ChunkSize = chunkSize;
        initReply->DiskFormat = NPDisk::TDiskFormatPtr(new NPDisk::TDiskFormat(format), +[](NPDisk::TDiskFormat* ptr) {
            delete ptr;
        });
        if (chunkMapSnapshot) {
            TString data;
            UNIT_ASSERT(chunkMapSnapshot->SerializeToString(&data));
            initReply->StartingPoints[TLogSignature::SignatureDDiskChunkMap] =
                NPDisk::TLogRecord(TLogSignature::SignatureDDiskChunkMap, TRcBuf(data), chunkMapSnapshotLsn);
        }
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
        for (const auto& [record, lsn] : replay) {
            TString data;
            UNIT_ASSERT(record.SerializeToString(&data));
            readLogReply->Results.emplace_back(
                TLogSignature::SignatureDDiskChunkMap, TRcBuf(data), lsn);
        }
        SendPDiskResponse(disk, *readLog, readLogReply.release());
        if (bootReclaimedChunks) {
            auto reclaim = WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
            *bootReclaimedChunks = reclaim->Get()->CommitRecord.DeleteChunks;
            ReplyLog(disk, *reclaim);
        }
        std::set<std::pair<ui64, ui64>> restoredDataChunks;
        std::set<std::pair<ui64, ui64>> restoredDataChunksWithExtents;
        bool hasIntegrityChunks = false;
        const auto inspectChunkMap = [&](const auto& chunkMap) {
            using TChunkMapLogRecord =
                NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
            switch (chunkMap.GetRecordCase()) {
                case TChunkMapLogRecord::kSnapshot:
                    for (const auto& tablet : chunkMap.GetSnapshot().GetTabletRecords()) {
                        for (const auto& chunk : tablet.GetChunkRefs()) {
                            if (!chunk.GetChunkIdx()) {
                                continue;
                            }
                            const auto key = std::make_pair(
                                tablet.GetTabletId(), chunk.GetVChunkIndex());
                            restoredDataChunks.insert(key);
                            if (chunk.HasExtentRef()) {
                                restoredDataChunksWithExtents.insert(key);
                            }
                        }
                    }
                    hasIntegrityChunks |=
                        chunkMap.GetSnapshot().IntegrityChunksSize() != 0;
                    break;
                case TChunkMapLogRecord::kIncrement: {
                    const auto& increment = chunkMap.GetIncrement();
                    const auto& dataChunk = increment.GetDataChunk();
                    if (dataChunk.GetChunkIdx()) {
                        const auto key = std::make_pair(
                            dataChunk.GetTabletId(), dataChunk.GetVChunkIndex());
                        restoredDataChunks.insert(key);
                        if (dataChunk.HasExtentRef()) {
                            restoredDataChunksWithExtents.insert(key);
                        }
                    }
                    hasIntegrityChunks |= increment.HasIntegrityChunk();
                    break;
                }
                default:
                    break;
            }
        };
        if (chunkMapSnapshot) {
            inspectChunkMap(*chunkMapSnapshot);
        }
        for (const auto& [record, lsn] : replay) {
            Y_UNUSED(lsn);
            inspectChunkMap(record);
        }
        const bool hasDataChunksWithoutExtents = std::any_of(
            restoredDataChunks.begin(),
            restoredDataChunks.end(),
            [&](const auto& key) {
                return !restoredDataChunksWithExtents.contains(key);
            });
        if ((!disk.EnableChecksums && hasIntegrityChunks)
                || (disk.EnableChecksums && hasDataChunksWithoutExtents)) {
            // The actor has entered Broken and does not perform normal reserve/PB bootstrap.
            return;
        }

        // DDisk bootstrap starts persistent buffer initialization in background.
        // Burn these PDisk requests here, so later client-only phases don't see unsolicited PDisk traffic.
        auto reserve = WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, MinChunksReserved);
        auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        const ui32 startupReserveChunks = PersistentBufferInitChunks + ddiskReserveChunks;
        for (ui32 i = 0; i < startupReserveChunks; ++i) {
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + i);
        }
        SendPDiskResponse(disk, *reserve, reserveReply.release());

        if (!disk.EnableChecksums) {
            ui32 pbLogs = 0;
            bool checkSpaceReplied = false;
            std::map<TChunkIdx, ui32> formattedBytes;
            std::set<TChunkIdx> formattedChunks;
            while (pbLogs < PersistentBufferInitChunks
                    || formattedChunks.size() < startupReserveChunks) {
                auto raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
                switch (raw->GetTypeRewrite()) {
                    case NPDisk::TEvChunkWriteRaw::EventType: {
                        auto write =
                            RecastEvent<NPDisk::TEvChunkWriteRaw>(std::move(raw));
                        const auto& request = *write->Get();
                        ui32& expectedOffset = formattedBytes[request.ChunkIdx];
                        UNIT_ASSERT_VALUES_EQUAL(request.Offset, expectedOffset);
                        for (auto it = request.Data.Begin(); it.Valid();
                                it.AdvanceToNextContiguousBlock()) {
                            const char* data = it.ContiguousData();
                            UNIT_ASSERT_C(
                                std::all_of(
                                    data,
                                    data + it.ContiguousSize(),
                                    [](char value) { return value == 0; }),
                                "chunk formatting must write only zeroes");
                        }
                        expectedOffset += request.Data.size();
                        UNIT_ASSERT(expectedOffset <= chunkSize);
                        if (expectedOffset == chunkSize) {
                            UNIT_ASSERT(
                                formattedChunks.insert(request.ChunkIdx).second);
                        }
                        SendPDiskResponse(
                            disk,
                            *write,
                            new NPDisk::TEvChunkWriteRawResult(
                                NKikimrProto::OK, ""));
                        break;
                    }
                    case NPDisk::TEvLog::EventType: {
                        auto log = RecastEvent<NPDisk::TEvLog>(std::move(raw));
                        UNIT_ASSERT(pbLogs < PersistentBufferInitChunks);
                        ReplyLog(disk, *log);
                        ++pbLogs;
                        break;
                    }
                    case NPDisk::TEvCheckSpace::EventType: {
                        auto checkSpace =
                            RecastEvent<NPDisk::TEvCheckSpace>(std::move(raw));
                        SendPDiskResponse(
                            disk,
                            *checkSpace,
                            new NPDisk::TEvCheckSpaceResult(
                                NKikimrProto::OK,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                "",
                                0));
                        checkSpaceReplied = true;
                        break;
                    }
                    default:
                        UNIT_FAIL(
                            "unexpected PDisk event during checksums-disabled boot: "
                            << raw->GetTypeRewrite());
                }
            }
            if (!checkSpaceReplied) {
                auto checkSpace =
                    WaitPDiskRequest<NPDisk::TEvCheckSpace>(disk);
                SendPDiskResponse(
                    disk,
                    *checkSpace,
                    new NPDisk::TEvCheckSpaceResult(
                        NKikimrProto::OK,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        "",
                        0));
            }
            return;
        }

        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            std::unique_ptr<TEventHandle<NPDisk::TEvLog>> log;
            if (ddiskReserveChunks < MinChunksReserved) {
                while (!log) {
                    auto raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
                    if (raw->GetTypeRewrite() == NPDisk::TEvChunkReserve::EventType) {
                        UNIT_ASSERT(!HeldBootstrapRefill);
                        HeldBootstrapRefill = RecastEvent<NPDisk::TEvChunkReserve>(std::move(raw));
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), NPDisk::TEvLog::EventType);
                    log = RecastEvent<NPDisk::TEvLog>(std::move(raw));
                }
            } else {
                log = WaitPDiskRequest<NPDisk::TEvLog>(disk);
            }
            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            SendPDiskResponse(disk, *log, logReply.release());
        }
        std::unique_ptr<TEventHandle<NPDisk::TEvCheckSpace>> checkSpace;
        if (ddiskReserveChunks < MinChunksReserved) {
            while (!checkSpace) {
                auto raw = Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
                if (raw->GetTypeRewrite() == NPDisk::TEvChunkReserve::EventType) {
                    UNIT_ASSERT(!HeldBootstrapRefill);
                    HeldBootstrapRefill = RecastEvent<NPDisk::TEvChunkReserve>(std::move(raw));
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), NPDisk::TEvCheckSpace::EventType);
                checkSpace = RecastEvent<NPDisk::TEvCheckSpace>(std::move(raw));
            }
            UNIT_ASSERT(HeldBootstrapRefill);
        } else {
            checkSpace = WaitPDiskRequest<NPDisk::TEvCheckSpace>(disk);
        }
        auto res = new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0);
        SendPDiskResponse(disk, *checkSpace, res);
    }
};

void SendToDDisk(TTestContext& ctx, const TActorId& serviceId, IEventBase* event, ui64 cookie = 0) {
    ctx.Runtime.Send(new IEventHandle(serviceId, ctx.Edge, event, 0, cookie), NodeId);
}

template<typename TResponseEvent>
std::unique_ptr<TEventHandle<TResponseEvent>> WaitFromDDisk(TTestContext& ctx) {
    for (;;) {
        std::unique_ptr<IEventHandle> raw = ctx.Runtime.WaitForEdgeActorEvent(ctx.ClientWaitEdges());
        if (ctx.ConsumeUnsolicitedPDiskEvent(raw)) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), TResponseEvent::EventType);
        return std::unique_ptr<TEventHandle<TResponseEvent>>(
            reinterpret_cast<TEventHandle<TResponseEvent>*>(raw.release()));
    }
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

std::vector<ui64> MakeBlockChecksums(const TString& data) {
    return NDDisk::CalculatePayloadChecksums(MakeAlignedRope(data));
}

TRope MakeMisalignedRope(const TString& data) {
    auto buf = TRcBuf::UninitializedPageAligned(data.size() + BlockSize);
    memcpy(buf.GetDataMut() + 1, data.data(), data.size());
    return TRope(TRcBuf(TRcBuf::Piece, buf.data() + 1, data.size(), buf));
}

TRope MakeRestoredIntegrityPair(ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId, ui64 vChunkIndex,
        ui64 vChunkGeneration, ui32 integrityChunkIdx, ui32 extentSlot,
        ui64 integrityChunkGeneration, const TString& blockData) {
    UNIT_ASSERT_VALUES_EQUAL(blockData.size(), BlockSize);
    const ui64 pureChecksum = NDDisk::CalculateRawChecksum(blockData.data(), blockData.size());
    NDDisk::TIntegrityBlock slots[NDDisk::IntegrityPairSlots]{};
    for (ui32 slotIdx = 0; slotIdx < NDDisk::IntegrityPairSlots; ++slotIdx) {
        auto& block = slots[slotIdx];
        auto& header = block.Header;
        header.Magic = NDDisk::MagicIntegrityBlock;
        header.FormatVersion = static_cast<ui16>(NDDisk::EIntegrityFormatVersion::BaseAwupf4KiB);
        header.ChecksumBlockIdx = 0;
        header.OwnerId = tabletId;
        header.VChunkId = vChunkIndex;
        header.VChunkGeneration = vChunkGeneration;
        header.IntegrityChunkId = integrityChunkIdx;
        header.IntegrityExtentId = extentSlot;
        header.IntegrityChunkGeneration = integrityChunkGeneration;
        header.IntegrityBlockDigest = NDDisk::Contribution(vChunkGeneration, 0, pureChecksum);
        header.PairSequenceNumber = slotIdx;
        header.UsedBlocksBitmap[0] = 1;
        block.Checksums[0] = NDDisk::SealBlockChecksum(
            pureChecksum, ddiskId, pdiskGuid, tabletId, vChunkIndex, 0);
        header.BlockChecksum = NDDisk::CalculateRawChecksum(&block, sizeof(block));
    }
    auto data = TRcBuf::UninitializedPageAligned(sizeof(slots));
    memcpy(data.GetDataMut(), slots, sizeof(slots));
    return TRope(std::move(data));
}

std::unique_ptr<NDDisk::TEvWrite> MakeWrite(const NDDisk::TQueryCredentials& creds,
        ui64 vChunkIndex, ui32 offset, const TString& payload) {
    auto write = std::make_unique<NDDisk::TEvWrite>(
        creds, NDDisk::TBlockSelector(vChunkIndex, offset, payload.size()), NDDisk::TWriteInstruction(0));
    write->AddPayloadThenChecksum(MakeAlignedRope(payload));
    return write;
}

NDDisk::TEvSync::TDDiskId MakeSyncSourceId(ui32 pdiskId, ui32 slotId) {
    return std::make_tuple(NodeId, pdiskId, slotId);
}

NDDisk::TQueryCredentials Connect(
        TTestContext& ctx,
        const TActorId& serviceId,
        ui64 tabletId,
        ui32 generation,
        ui32 directBlockGroupIndex = 0
) {
    const bool isPersistentBuffer = serviceId.IsService() && serviceId.ServiceId().StartsWith("NPB_");
    NDDisk::TQueryCredentials creds = isPersistentBuffer
        ? NDDisk::TQueryCredentials::ToPersistentBuffer(tabletId, generation, std::nullopt, directBlockGroupIndex)
        : NDDisk::TQueryCredentials::ToDDisk(tabletId, generation, 0, std::nullopt, directBlockGroupIndex);

    auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, serviceId, new NDDisk::TEvConnect(creds));
    AssertStatus(connectResult, TReplyStatus::OK);
    creds.DDiskInstanceGuid = connectResult->Get()->Record.GetDDiskInstanceGuid();
    creds.ConnectionToken.emplace(connectResult->Get()->Record.GetConnectionToken());

    return creds;
}

void AssertNoClientReplyBeforeSentinel(TTestContext& ctx, TStringBuf message) {
    const TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
    ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
    for (;;) {
        auto ev = ctx.Runtime.WaitForEdgeActorEvent(ctx.ClientWaitEdges({sentinelEdge}));
        if (ctx.ConsumeUnsolicitedPDiskEvent(ev)) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge, message);
        break;
    }
}

struct TInitialWriteOutcome {
    std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>> WriteResult;
    ui32 ChunkIdx = 0;
};

/** First write to a vchunk: formatting I/O, the data write and the combined increment run in
 * parallel. Integrity metadata writes and reserve refills are auto-served. The client reply is
 * gated on the increment becoming durable. */
TInitialWriteOutcome DoWriteWithChunkAllocation(TTestContext& ctx, const TDiskHandle& disk, std::unique_ptr<NDDisk::TEvWrite> write,
        ui32 chunkId, ui32 expectedOffsetInBytes, const TString& expectedPayload,
        bool reserveExpected, bool checkSnapshot) {
    SendToDDisk(ctx, disk.ServiceId, write.release());

    if (!reserveExpected) {
        // no existing reserve: have to request chunks (the reserve is refilled up to MinChunksReserved)
        auto refill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        UNIT_ASSERT_VALUES_EQUAL(refill->Get()->SizeChunks, MinChunksReserved);

        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        for (ui32 i = 0; i < MinChunksReserved; ++i) {
            refillReply->ChunkIds.push_back(chunkId + i);
        }
        ctx.SendPDiskResponse(disk, *refill, refillReply.release());
    }

    auto traffic = ctx.CollectAllocationTraffic(disk, checkSnapshot, 1);
    UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
    UNIT_ASSERT(traffic.Increment);
    const ui32 chunkIdx = traffic.DataWrites[0]->Get()->ChunkIdx;
    UNIT_ASSERT(chunkIdx != 0u);
    UNIT_ASSERT_VALUES_EQUAL(chunkIdx, chunkId);
    UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, expectedOffsetInBytes);
    UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), expectedPayload);

    // Data I/O first: the write result must stay parked until the increment commits.
    ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
    ctx.ReplyLog(disk, *traffic.Increment);

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
    Y_UNIT_TEST(PoisonNotifiesNodeWardenImmediately) {
        TTestContext ctx;
        ctx.Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), ctx.Edge);

        const TDiskHandle disk = ctx.CreateDDisk(43, 1);
        const TActorId ddiskActorId =
            ctx.Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(disk.ServiceId);
        const TActorId persistentBufferActorId =
            ctx.Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(disk.PBServiceId);
        UNIT_ASSERT(ddiskActorId);
        UNIT_ASSERT(persistentBufferActorId);

        std::unique_ptr<IEventHandle> blockedPersistentBufferPoison;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (!blockedPersistentBufferPoison &&
                    ev->GetTypeRewrite() == TEvents::TSystem::Poison &&
                    ev->Recipient == persistentBufferActorId) {
                blockedPersistentBufferPoison = std::move(ev);
                return false;
            }
            return true;
        };

        SendToDDisk(ctx, disk.ServiceId, new TEvents::TEvPoison());
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return !blockedPersistentBufferPoison && ++eventsProcessed <= 200;
        });
        UNIT_ASSERT_C(blockedPersistentBufferPoison, "DDisk must poison its persistent buffer actor");
        UNIT_ASSERT(!ctx.Runtime.WrapInActorContext(ddiskActorId, [](IActor*) {}));
        UNIT_ASSERT(ctx.Runtime.WrapInActorContext(persistentBufferActorId, [](IActor*) {}));

        ctx.Runtime.FilterFunction = {};
        const auto gone = WaitFromDDisk<TEvents::TEvGone>(ctx);

        ctx.Runtime.Send(std::move(blockedPersistentBufferPoison), NodeId);

        UNIT_ASSERT_VALUES_EQUAL(gone->Sender, ddiskActorId);
        UNIT_ASSERT(!ctx.Runtime.WrapInActorContext(ddiskActorId, [](IActor*) {}));
        ui32 persistentBufferEventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return ctx.Runtime.WrapInActorContext(persistentBufferActorId, [](IActor*) {})
                && ++persistentBufferEventsProcessed <= 200;
        });
        UNIT_ASSERT_C(!ctx.Runtime.WrapInActorContext(persistentBufferActorId, [](IActor*) {}),
            "Persistent buffer must stop after receiving poison");
    }

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
        creds.ConnectionToken.emplace(connectResult->Get()->Record.GetConnectionToken());

        auto disconnect = std::make_unique<NDDisk::TEvDisconnect>();
        creds.SerializeForRequest(disconnect->Record.MutableCredentials());
        auto disconnectResult = SendToDDiskAndWait<NDDisk::TEvDisconnectResult>(ctx, disk.ServiceId,
            disconnect.release());
        AssertStatus(disconnectResult, TReplyStatus::OK);

        auto readAfterDisconnect = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(readAfterDisconnect, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(ConnectionTokenBitLayout) {
        // Verify the exact 128-bit connection token layout:
        // 1. Pack distinct values into every field.
        // 2. Check the resulting Low and High words.
        // 3. Decode every field and compare it with the original value.
        NDDisk::TConnectionToken token = NDDisk::TConnectionToken::Make(
            0x1122'3344,
            0x55,
            0x6677'8899,
            0xaabb,
            0xccdd,
            0xeeff,
            0x12
        );

        UNIT_ASSERT_VALUES_EQUAL(0x6677'8899'1122'3344, token.Low);
        UNIT_ASSERT_VALUES_EQUAL(0xeeff'ccdd'aabb'1255, token.High);
        UNIT_ASSERT_VALUES_EQUAL(0x1122'3344, token.GetConnectionIndex());
        UNIT_ASSERT_VALUES_EQUAL(0x55, token.GetSequenceNo());
        UNIT_ASSERT_VALUES_EQUAL(0x6677'8899, token.GetTabletIdSuffix());
        UNIT_ASSERT_VALUES_EQUAL(0xaabb, token.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(0xccdd, token.GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(0xeeff, token.GetVSlotId());
        UNIT_ASSERT_VALUES_EQUAL(0x12, token.GetRandom());
    }

    Y_UNIT_TEST(ConnectionTokenValidation) {
        // Verify token validation and bounded stale-token history:
        // 1. Connect and check the issued token's slot and identity fields.
        // 2. Corrupt the token and reject it as invalid.
        // 3. Repeat the same connect idempotently, then reconnect in the same
        //    slot with a larger session sequence and rotate the token.
        // 4. Rotate twice more and check that the two recent tokens are stale
        //    while the oldest token has fallen out of bounded history.
        // 5. Use the current token successfully and verify the external request
        //    contains no server-side context.
        // 6. Disconnect, reuse the freed slot, and keep the disconnected token
        //    classified as stale.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(2, 4);

        constexpr ui64 TabletId = 0x1234'5678'9abc'def0;
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, TabletId, 1);
        UNIT_ASSERT(creds.ConnectionToken);

        const NDDisk::TConnectionToken firstToken = *creds.ConnectionToken;
        UNIT_ASSERT_VALUES_EQUAL(0, firstToken.GetConnectionIndex());
        UNIT_ASSERT_VALUES_EQUAL(1, firstToken.GetSequenceNo());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(TabletId), firstToken.GetTabletIdSuffix());
        UNIT_ASSERT_VALUES_EQUAL(NodeId, firstToken.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(disk.PDiskId, firstToken.GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(disk.SlotId, firstToken.GetVSlotId());

        NDDisk::TQueryCredentials corrupted = creds;
        corrupted.ConnectionToken->High ^= 1ull << 32;
        auto corruptedTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(corrupted, {0, 0, BlockSize}, {true})
        );
        AssertStatus(corruptedTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("invalid connection token", corruptedTokenRead->Get()->Record.GetErrorReason());

        auto reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(creds)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);

        NDDisk::TQueryCredentials reconnected = creds;
        reconnected.ConnectionToken.emplace(reconnectResult->Get()->Record.GetConnectionToken());
        UNIT_ASSERT(firstToken == *reconnected.ConnectionToken);

        reconnected.DDiskSessionSeqNo++;
        reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(reconnected)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);
        reconnected.ConnectionToken.emplace(reconnectResult->Get()->Record.GetConnectionToken());
        UNIT_ASSERT_VALUES_EQUAL(firstToken.GetConnectionIndex(), reconnected.ConnectionToken->GetConnectionIndex());
        UNIT_ASSERT_VALUES_EQUAL(firstToken.GetSequenceNo() + 1, reconnected.ConnectionToken->GetSequenceNo());
        UNIT_ASSERT(firstToken != *reconnected.ConnectionToken);
        const NDDisk::TConnectionToken secondToken = *reconnected.ConnectionToken;

        auto obsoleteTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(obsoleteTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("stale connection token", obsoleteTokenRead->Get()->Record.GetErrorReason());

        reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(reconnected)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);
        auto expectedToken = NDDisk::TConnectionToken(reconnectResult->Get()->Record.GetConnectionToken());
        UNIT_ASSERT(*reconnected.ConnectionToken == expectedToken);

        reconnected.DDiskSessionSeqNo++;
        reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(reconnected)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);

        reconnected.ConnectionToken.emplace(reconnectResult->Get()->Record.GetConnectionToken());
        obsoleteTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(obsoleteTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("stale connection token", obsoleteTokenRead->Get()->Record.GetErrorReason());

        reconnected.DDiskSessionSeqNo++;
        reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(reconnected)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);

        reconnected.ConnectionToken.emplace(reconnectResult->Get()->Record.GetConnectionToken());
        obsoleteTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(obsoleteTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("invalid connection token", obsoleteTokenRead->Get()->Record.GetErrorReason());

        NDDisk::TQueryCredentials secondTokenCreds = creds;
        secondTokenCreds.ConnectionToken = secondToken;
        auto secondTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(secondTokenCreds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(secondTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("stale connection token", secondTokenRead->Get()->Record.GetErrorReason());

        auto currentTokenRequest = std::make_unique<NDDisk::TEvRead>(
            reconnected,
            NDDisk::TBlockSelector{0, 0, BlockSize},
            NDDisk::TReadInstruction{true}
        );
        const auto& requestCredentials = currentTokenRequest->Record.GetCredentials();
        UNIT_ASSERT(requestCredentials.HasConnectionToken());
        UNIT_ASSERT(!requestCredentials.HasInternal());

        auto currentTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            currentTokenRequest.release()
        );
        AssertStatus(currentTokenRead, TReplyStatus::OK);

        auto disconnect = std::make_unique<NDDisk::TEvDisconnect>();
        reconnected.SerializeForRequest(disconnect->Record.MutableCredentials());
        auto disconnectResult = SendToDDiskAndWait<NDDisk::TEvDisconnectResult>(
            ctx,
            disk.ServiceId,
            disconnect.release()
        );
        AssertStatus(disconnectResult, TReplyStatus::OK);

        NDDisk::TQueryCredentials reused = Connect(ctx, disk.ServiceId, TabletId + 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(firstToken.GetConnectionIndex(), reused.ConnectionToken->GetConnectionIndex());
        UNIT_ASSERT_VALUES_EQUAL(reconnected.ConnectionToken->GetSequenceNo() + 1, reused.ConnectionToken->GetSequenceNo());

        auto invalidatedTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(reconnected, {0, 0, BlockSize}, {true})
        );
        AssertStatus(invalidatedTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("stale connection token", invalidatedTokenRead->Get()->Record.GetErrorReason());
    }

    Y_UNIT_TEST(ConnectionTokenSequenceWraps) {
        // Verify that the 8-bit token sequence wraps without breaking a slot:
        // 1. Establish a connection and remember its vector index.
        // 2. Reconnect in the same slot until the token sequence reaches 255.
        // 3. Reconnect once more and check that zero is skipped and sequence 1
        //    is issued for the same vector index.
        // 4. Use the new token successfully and reject the preceding token as
        //    stale.
        TTestContext ctx;
        TDiskHandle disk = ctx.CreateDDisk(2, 6);

        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 0x1234'5678'9abc'def0, 1);
        ui32 connectionIndex = creds.ConnectionToken->GetConnectionIndex();
        std::optional<NDDisk::TConnectionToken> tokenBeforeWrap;

        for (ui32 i = 0; i < 255; ++i) {
            if (i == 254) {
                tokenBeforeWrap = creds.ConnectionToken;
                UNIT_ASSERT_VALUES_EQUAL(Max<ui8>(), tokenBeforeWrap->GetSequenceNo());
            }

            ++creds.DDiskSessionSeqNo;
            auto reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
                ctx,
                disk.ServiceId,
                new NDDisk::TEvConnect(creds)
            );
            AssertStatus(reconnectResult, TReplyStatus::OK);

            const NDDisk::TConnectionToken nextToken(reconnectResult->Get()->Record.GetConnectionToken());
            UNIT_ASSERT_VALUES_EQUAL(connectionIndex, nextToken.GetConnectionIndex());
            UNIT_ASSERT(*creds.ConnectionToken != nextToken);
            creds.ConnectionToken = nextToken;
        }

        UNIT_ASSERT(tokenBeforeWrap);
        UNIT_ASSERT_VALUES_EQUAL(1, creds.ConnectionToken->GetSequenceNo());

        auto currentTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(currentTokenRead, TReplyStatus::OK);

        NDDisk::TQueryCredentials staleCreds = creds;
        staleCreds.ConnectionToken = tokenBeforeWrap;
        auto staleTokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(staleCreds, {0, 0, BlockSize}, {true})
        );
        AssertStatus(staleTokenRead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL(
            "stale connection token",
            staleTokenRead->Get()->Record.GetErrorReason()
        );
    }

    Y_UNIT_TEST(ConnectionTokenSeparatesDirectBlockGroups) {
        // Verify independent connection slots for two DBGs of one tablet:
        // 1. Connect two DBGs and check that their slots and tokens differ.
        // 2. Repeat DBG B's connect idempotently.
        // 3. Reconnect DBG A in its original slot, invalidate only A's old
        //    token, and keep DBG B usable.
        // 4. Disconnect DBG A, reuse its freed slot for DBG C, and verify DBG B
        //    is still usable.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(2, 5);

        constexpr ui64 TabletId = 0x1234'5678'9abc'def0;
        NDDisk::TQueryCredentials groupA = Connect(ctx, disk.ServiceId, TabletId, 1, 10);
        NDDisk::TQueryCredentials groupB = Connect(ctx, disk.ServiceId, TabletId, 1, 11);

        UNIT_ASSERT(groupA.ConnectionToken);
        UNIT_ASSERT(groupB.ConnectionToken);
        UNIT_ASSERT_VALUES_UNEQUAL(
            groupA.ConnectionToken->GetConnectionIndex(),
            groupB.ConnectionToken->GetConnectionIndex()
        );
        UNIT_ASSERT(*groupA.ConnectionToken != *groupB.ConnectionToken);

        auto duplicateConnect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(groupB)
        );
        AssertStatus(duplicateConnect, TReplyStatus::OK);
        auto expectedToken = NDDisk::TConnectionToken(duplicateConnect->Get()->Record.GetConnectionToken());
        UNIT_ASSERT(*groupB.ConnectionToken == expectedToken);

        NDDisk::TQueryCredentials reconnectedA = groupA;
        ++reconnectedA.DDiskSessionSeqNo;
        auto reconnectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvConnect(reconnectedA)
        );
        AssertStatus(reconnectResult, TReplyStatus::OK);
        reconnectedA.ConnectionToken.emplace(reconnectResult->Get()->Record.GetConnectionToken());
        UNIT_ASSERT_VALUES_EQUAL(
            groupA.ConnectionToken->GetConnectionIndex(),
            reconnectedA.ConnectionToken->GetConnectionIndex()
        );
        UNIT_ASSERT(*groupA.ConnectionToken != *reconnectedA.ConnectionToken);

        auto staleGroupARead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(groupA, {0, 0, BlockSize}, {true})
        );
        AssertStatus(staleGroupARead, TReplyStatus::SESSION_MISMATCH);
        UNIT_ASSERT_VALUES_EQUAL("stale connection token", staleGroupARead->Get()->Record.GetErrorReason());

        auto groupBRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(groupB, {0, 0, BlockSize}, {true})
        );
        AssertStatus(groupBRead, TReplyStatus::OK);

        auto disconnectA = std::make_unique<NDDisk::TEvDisconnect>();
        reconnectedA.SerializeForRequest(disconnectA->Record.MutableCredentials());
        auto disconnectResult = SendToDDiskAndWait<NDDisk::TEvDisconnectResult>(
            ctx,
            disk.ServiceId,
            disconnectA.release()
        );
        AssertStatus(disconnectResult, TReplyStatus::OK);

        NDDisk::TQueryCredentials groupC = Connect(ctx, disk.ServiceId, TabletId, 1, 12);
        UNIT_ASSERT_VALUES_EQUAL(
            reconnectedA.ConnectionToken->GetConnectionIndex(),
            groupC.ConnectionToken->GetConnectionIndex()
        );

        groupBRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(groupB, {0, 0, BlockSize}, {true})
        );
        AssertStatus(groupBRead, TReplyStatus::OK);
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
        gen2.ConnectionToken.emplace(gen2Connect->Get()->Record.GetConnectionToken());
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
        gen3.ConnectionToken.emplace(gen3Connect->Get()->Record.GetConnectionToken());

        auto queryWithLatestGeneration = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(gen3, {0, 0, BlockSize}, {true}));
        AssertStatus(queryWithLatestGeneration, TReplyStatus::OK);

        auto queryWithOldGeneration = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(gen2, {0, 0, BlockSize}, {true}));
        AssertStatus(queryWithOldGeneration, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(ConnectSessionSeqNoRules) {
        // Scenario: reject an older session, rotate the token for a newer one,
        // accept matching internal context, then reject stale generation and
        // instance identity from internal forwarding.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(2, 2);

        NDDisk::TQueryCredentials seq1 = NDDisk::TQueryCredentials::ToDDisk(12, 4, 1, std::nullopt, 0);
        auto seq1Connect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(seq1));
        AssertStatus(seq1Connect, TReplyStatus::OK);
        seq1.DDiskInstanceGuid = seq1Connect->Get()->Record.GetDDiskInstanceGuid();
        seq1.ConnectionToken.emplace(seq1Connect->Get()->Record.GetConnectionToken());

        NDDisk::TQueryCredentials obsoleteSeq = seq1;
        obsoleteSeq.DDiskSessionSeqNo = 0;
        auto obsoleteConnect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(obsoleteSeq));
        AssertStatus(obsoleteConnect, TReplyStatus::BLOCKED);

        NDDisk::TQueryCredentials seq2 = seq1;
        seq2.DDiskSessionSeqNo = 2;
        auto seq2Connect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.ServiceId, new NDDisk::TEvConnect(seq2));
        AssertStatus(seq2Connect, TReplyStatus::OK);
        seq2.DDiskInstanceGuid = seq2Connect->Get()->Record.GetDDiskInstanceGuid();
        seq2.ConnectionToken.emplace(seq2Connect->Get()->Record.GetConnectionToken());

        auto oldSessionRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(seq1, {0, 0, BlockSize}, {true}));
        AssertStatus(oldSessionRead, TReplyStatus::SESSION_MISMATCH);

        auto newSessionRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(seq2, {0, 0, BlockSize}, {true}));
        AssertStatus(newSessionRead, TReplyStatus::OK);

        auto internalRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                NDDisk::TQueryCredentials::ForInternal(12, 4, std::nullopt, 0),
                {0, 0, BlockSize},
                {true}));
        AssertStatus(internalRead, TReplyStatus::OK);

        auto staleInternalRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                NDDisk::TQueryCredentials::ForInternal(12, 3, std::nullopt, 0),
                {0, 0, BlockSize},
                {true}
            )
        );
        AssertStatus(staleInternalRead, TReplyStatus::SESSION_MISMATCH);

        auto wrongInstanceRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                NDDisk::TQueryCredentials::ForInternal(12, 4, *seq2.DDiskInstanceGuid + 1, 0),
                {0, 0, BlockSize},
                {true}
            )
        );
        AssertStatus(wrongInstanceRead, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(PersistentBufferConnectIsHandledByPersistentBufferActor) {
        // Scenario: connect directly to the PB actor, use its token for a PB
        // request, reject that token at DDisk, then disconnect from PB.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(2, 3);

        NDDisk::TQueryCredentials creds = NDDisk::TQueryCredentials::ToPersistentBuffer(12, 4, std::nullopt, 0);

        auto wrongDDiskConnect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, disk.ServiceId, new NDDisk::TEvConnect(creds));
        AssertStatus(wrongDDiskConnect, TReplyStatus::INCORRECT_REQUEST);

        auto ddiskCreds = NDDisk::TQueryCredentials::ToDDisk(12, 4, 1, std::nullopt, 0);
        auto wrongPBConnect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, disk.PBServiceId, new NDDisk::TEvConnect(ddiskCreds));
        AssertStatus(wrongPBConnect, TReplyStatus::INCORRECT_REQUEST);

        auto connect = SendToDDiskAndWait<NDDisk::TEvConnectResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvConnect(creds));
        AssertStatus(connect, TReplyStatus::OK);
        creds.DDiskInstanceGuid = connect->Get()->Record.GetDDiskInstanceGuid();
        creds.ConnectionToken.emplace(connect->Get()->Record.GetConnectionToken());

        auto list = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx,
            disk.PBServiceId,
            new NDDisk::TEvListPersistentBuffer(creds)
        );
        AssertStatus(list, TReplyStatus::OK);

        auto ddiskRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {0, 0, BlockSize},
                {true}
            )
        );
        AssertStatus(ddiskRead, TReplyStatus::SESSION_MISMATCH);

        NDDisk::TQueryCredentials mismatchedSeq = creds;
        mismatchedSeq.DDiskSessionSeqNo = 42;
        auto disconnect = std::make_unique<NDDisk::TEvDisconnect>();
        mismatchedSeq.SerializeForRequest(disconnect->Record.MutableCredentials());
        auto disconnectResult = SendToDDiskAndWait<NDDisk::TEvDisconnectResult>(
            ctx, disk.PBServiceId, disconnect.release());
        AssertStatus(disconnectResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(IncorrectRequestValidation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(3, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 10, 1);

        auto misaligned = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 1, BlockSize),
            NDDisk::TWriteInstruction(0));
        misaligned->AddPayloadThenChecksum(TRope(MakeData('A', BlockSize)));
        auto misalignedResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(ctx, disk.ServiceId, misaligned.release());
        AssertStatus(misalignedResult, TReplyStatus::INCORRECT_REQUEST);

        auto wrongSize = std::make_unique<NDDisk::TEvWrite>(creds, NDDisk::TBlockSelector(0, 0, BlockSize),
            NDDisk::TWriteInstruction(0));
        wrongSize->AddPayloadThenChecksum(TRope(MakeData('B', 2 * BlockSize)));
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
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(0), NDDisk::GetZeroBlockChecksum());
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(1), NDDisk::GetZeroBlockChecksum());
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
            write->AddPayloadThenChecksum(TRope(MakeData('W', BlockSize)));
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
        write->AddPayloadThenChecksum(TRope(MakeData('W', 2 * BlockSize)));
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
            write->AddPayloadThenChecksum(MakeMisalignedRope(MakeData('U', BlockSize)));
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
            write->AddPayloadThenChecksum(std::move(nonContiguous));
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
        write->AddPayloadThenChecksum(MakeAlignedRope(payload));

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
        const auto expectedChecksums = NDDisk::CalculatePayloadChecksums(MakeAlignedRope(payload));
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }

        // Second write to the same vchunk: only TEvChunkWriteRaw (DoWriteWithChunkAllocation's log/reserve path must not run)

        const TString payload2 = MakeData('R', 2 * BlockSize);
        const ui32 secondOffset = BlockSize + static_cast<ui32>(payload.size());
        auto write2 = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, secondOffset, static_cast<ui32>(payload2.size())), NDDisk::TWriteInstruction(0));
        write2->AddPayloadThenChecksum(MakeAlignedRope(payload2));
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
        const auto expectedChecksums2 = NDDisk::CalculatePayloadChecksums(MakeAlignedRope(payload2));
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Get()->Record.ChecksumsSize(), expectedChecksums2.size());
        for (ui32 i = 0; i < expectedChecksums2.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(readResult2->Get()->Record.GetChecksums(i), expectedChecksums2[i]);
        }
    }

    Y_UNIT_TEST(WriteReplyWaitsForIntegrityPairDurability) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 2);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 31, 1);

        const TString firstPayload = MakeData('A', BlockSize);
        auto first = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, firstPayload),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, firstPayload, true, true);
        AssertStatus(first.WriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, MakeWrite(creds, 0, BlockSize, MakeData('B', BlockSize)).release());
        auto write1 = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto write2 = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto* dataWrite = write1->Get()->ChunkIdx == first.ChunkIdx ? write1.get() : write2.get();
        auto* integrityWrite = write1->Get()->ChunkIdx == first.ChunkIdx ? write2.get() : write1.get();
        UNIT_ASSERT_VALUES_UNEQUAL(dataWrite->Get()->ChunkIdx, integrityWrite->Get()->ChunkIdx);

        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertNoClientReplyBeforeSentinel(ctx,
            "write reply must wait for the integrity pair image");

        ctx.SendPDiskResponse(disk, *integrityWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(WriteReplyWaitsForDataWhenIntegrityCompletesFirst) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(75, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 253, 1);

        const TString firstPayload = MakeData('A', BlockSize);
        auto first = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, firstPayload),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, firstPayload, true, true);
        AssertStatus(first.WriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, BlockSize, MakeData('B', BlockSize)).release());
        auto write1 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto write2 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto* dataWrite =
            write1->Get()->ChunkIdx == first.ChunkIdx ? write1.get() : write2.get();
        auto* integrityWrite =
            write1->Get()->ChunkIdx == first.ChunkIdx ? write2.get() : write1.get();

        bool sawWriteReply = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDDisk::TEvWriteResult::EventType
                    && ev->GetRecipientRewrite() == ctx.Edge) {
                sawWriteReply = true;
            }
            return true;
        };
        ctx.SendPDiskResponse(disk, *integrityWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertNoClientReplyBeforeSentinel(
            ctx, "write reply must also wait when integrity completes before data");
        UNIT_ASSERT(!sawWriteReply);

        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto result = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT(sawWriteReply);
        AssertStatus(result, TReplyStatus::OK);
    }

    Y_UNIT_TEST(SameExtentWritesSerializeDataAndIntegrity) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 4);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 33, 1);

        const TString initialPayload = MakeData('A', BlockSize);
        auto initial = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, initialPayload),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, initialPayload, true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        std::vector<std::unique_ptr<IEventHandle>> heldWrites;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType) {
                heldWrites.push_back(std::move(ev));
                return false;
            }
            return true;
        };

        const TString firstPayload = MakeData('B', BlockSize);
        const TString secondPayload = MakeData('C', BlockSize);
        SendToDDisk(ctx, disk.ServiceId, MakeWrite(creds, 0, 0, firstPayload).release());
        SendToDDisk(ctx, disk.ServiceId, MakeWrite(creds, 0, 0, secondPayload).release());
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return heldWrites.size() < 2 && ++eventsProcessed <= 200;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(heldWrites.size(), 2,
            "only the first write's data and integrity I/O may be submitted");

        auto firstRaw = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
            reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(heldWrites[0].release()));
        auto secondRaw = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
            reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(heldWrites[1].release()));
        auto* firstData = firstRaw->Get()->ChunkIdx == initial.ChunkIdx ? firstRaw.get() : secondRaw.get();
        auto* firstIntegrity = firstRaw->Get()->ChunkIdx == initial.ChunkIdx ? secondRaw.get() : firstRaw.get();
        UNIT_ASSERT_VALUES_EQUAL(firstData->Get()->Data.ConvertToString(), firstPayload);

        heldWrites.clear();
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType) {
                heldWrites.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        ctx.SendPDiskResponse(disk, *firstData,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *firstIntegrity,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
        eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return heldWrites.size() < 2 && ++eventsProcessed <= 200;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(heldWrites.size(), 2);

        auto nextRaw1 = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
            reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(heldWrites[0].release()));
        auto nextRaw2 = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
            reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(heldWrites[1].release()));
        auto* secondData = nextRaw1->Get()->ChunkIdx == initial.ChunkIdx ? nextRaw1.get() : nextRaw2.get();
        auto* secondIntegrity = nextRaw1->Get()->ChunkIdx == initial.ChunkIdx ? nextRaw2.get() : nextRaw1.get();
        UNIT_ASSERT_VALUES_EQUAL(secondData->Get()->Data.ConvertToString(), secondPayload);

        ctx.SendPDiskResponse(disk, *secondData,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *secondIntegrity,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        ctx.SendPDiskResponse(
            disk, *dataRead, new NPDisk::TEvChunkReadRawResult(TRope(secondPayload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        const auto expectedChecksums =
            NDDisk::CalculatePayloadChecksums(MakeAlignedRope(secondPayload));
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(0), expectedChecksums[0]);
    }

    Y_UNIT_TEST(DifferentExtentWritesRemainConcurrent) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 6);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 35, 1);
        const ui32 firstChunk = disk.FirstChunkId + PersistentBufferInitChunks;

        auto first = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)),
            firstChunk, 0, MakeData('A', BlockSize), true, true);
        AssertStatus(first.WriteResult, TReplyStatus::OK);
        auto second = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 1, 0, MakeData('B', BlockSize)),
            firstChunk + 2, 0, MakeData('B', BlockSize), true, false);
        AssertStatus(second.WriteResult, TReplyStatus::OK);

        std::vector<std::unique_ptr<IEventHandle>> heldWrites;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType) {
                heldWrites.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, BlockSize, MakeData('C', BlockSize)).release());
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 1, BlockSize, MakeData('D', BlockSize)).release());
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return heldWrites.size() < 4 && ++eventsProcessed <= 300;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(heldWrites.size(), 4,
            "two different extents must submit both data+integrity batches concurrently");

        bool sawFirstData = false;
        bool sawSecondData = false;
        for (auto& raw : heldWrites) {
            auto write = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
                reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(raw.release()));
            if (write->Get()->ChunkIdx == first.ChunkIdx) {
                sawFirstData = true;
            } else if (write->Get()->ChunkIdx == second.ChunkIdx) {
                sawSecondData = true;
            }
            ctx.SendPDiskResponse(disk, *write,
                new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        }
        UNIT_ASSERT(sawFirstData);
        UNIT_ASSERT(sawSecondData);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(IntegrityWriteFailureBreaksDDiskAndFailsPendingWrite) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(5, 3);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 32, 1);

        const TString firstPayload = MakeData('A', BlockSize);
        auto first = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, firstPayload),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, firstPayload, true, true);
        AssertStatus(first.WriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, BlockSize, MakeData('B', BlockSize)).release());
        auto write1 = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto write2 = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto* dataWrite = write1->Get()->ChunkIdx == first.ChunkIdx ? write1.get() : write2.get();
        auto* integrityWrite = write1->Get()->ChunkIdx == first.ChunkIdx ? write2.get() : write1.get();
        UNIT_ASSERT_VALUES_UNEQUAL(dataWrite->Get()->ChunkIdx, integrityWrite->Get()->ChunkIdx);

        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *integrityWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::ERROR, "injected integrity failure"));

        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::ERROR);
        auto read = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(read, TReplyStatus::ERROR);
    }

    Y_UNIT_TEST(FallbackIntegrityReadFailureBreaksDDiskAndReplies) {
        TTestContext ctx;
        NDDisk::TDDiskConfig config;
        config.ForcePDiskFallback = true;
        config.IntegrityChecksumCacheBytes = NDDisk::TIntegrityManager::BlockStateApproxBytes;
        const TDiskHandle disk = ctx.CreateDDisk(5, 5, std::nullopt, std::move(config));
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 34, 1);

        const TString firstPayload = MakeData('A', BlockSize);
        auto initial = DoWriteWithChunkAllocation(ctx, disk,
            MakeWrite(creds, 0, 0, firstPayload),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, firstPayload, true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        const ui32 secondPairOffset =
            NDDisk::ChecksumsPerIntegrityBlock * NDDisk::IntegrityUnitSize;
        AssertStatus(
            DoWrite(ctx, disk, MakeWrite(
                creds, 0, secondPairOffset, MakeData('B', BlockSize))),
            TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        auto integrityRead = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_UNEQUAL(integrityRead->Get()->ChunkIdx, initial.ChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            integrityRead->Get()->Size,
            NDDisk::IntegrityPairSlots * NDDisk::IntegrityUnitSize);
        ctx.SendPDiskResponse(disk, *integrityRead,
            new NPDisk::TEvChunkReadRawResult(
                NKikimrProto::ERROR, "injected integrity read failure"));

        AssertStatus(WaitFromDDisk<NDDisk::TEvReadResult>(ctx), TReplyStatus::ERROR);
        auto laterRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(laterRead, TReplyStatus::ERROR);
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
        // Tablet1's allocation consumed two reserve chunks (data + integrity); tablet2's data
        // chunk is therefore the third one (its integrity extent reuses tablet1's integrity chunk).
        const ui32 chunkTablet2 = disk.FirstChunkId + PersistentBufferInitChunks + 2;

        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk.ServiceId, 101, 1);
        {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds1, NDDisk::TBlockSelector(0, 0, BlockSize),
                NDDisk::TWriteInstruction(0));
            w->AddPayloadThenChecksum(MakeAlignedRope(payload1));
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
            w->AddPayloadThenChecksum(MakeAlignedRope(payload2));
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
        write->AddPayloadThenChecksum(TRope(payload));
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

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn));

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        auto missingRead = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, selector, lsn, 1, {true}));
        AssertStatus(missingRead, TReplyStatus::MISSING_RECORD);

    }

    // TEvListPersistentBuffer must not observe a partially-applied write for its tablet: it has to
    // wait for any in-flight persistent-buffer disk operation belonging to that tablet to finish
    // before replying. Regression test for that ordering guarantee.
    Y_UNIT_TEST(PersistentBufferListWaitsForInflightWrite) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        // The write's disk op is now in flight (not yet acked by PDisk). Issue the list request for
        // the same tablet while it is still in flight: it must be deferred and only answered once the
        // write completes, never with a stale/partial view.
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));

        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        auto listResult = WaitFromDDisk<NDDisk::TEvListPersistentBufferResult>(ctx);
        AssertStatus(listResult, TReplyStatus::OK);
        // The list must reflect the completed write (i.e. it waited for the inflight to drain),
        // not the state as it was before the write finished.
        UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 1);
        const auto& record = listResult->Get()->Record.GetRecords(0);
        UNIT_ASSERT_VALUES_EQUAL(record.GetLsn(), lsn);
    }

    // Once retries are exhausted while the tablet's persistent-buffer disk operation is still in
    // flight, TEvListPersistentBuffer must reply with an error (not hang, and not answer with a
    // possibly-stale view).
    Y_UNIT_TEST(PersistentBufferListRepliesErrorAfterRetriesExhausted) {
        TTestContext ctx;
        NDDisk::TPersistentBufferFormat fmt;
        fmt.MaxChunks = 256;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 512 * 1024;
        fmt.ListPersistentBufferMaxRetries = 2;
        fmt.ListPersistentBufferRetryPeriodMilliseconds = 5;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        // Leave the write's disk op in flight (never ack it) and issue a list request for the same
        // tablet: it must keep retrying, then give up and reply with an error once retries run out.
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));

        auto listResult = WaitFromDDisk<NDDisk::TEvListPersistentBufferResult>(ctx);
        AssertStatus(listResult, TReplyStatus::OVERLOADED);

        // Complete the write afterwards so the test tears down cleanly.
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
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
        write->AddPayloadThenChecksum(TRope(payload));
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
        write->AddPayloadThenChecksum(TRope(payload));
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
        write->AddPayloadThenChecksum(TRope(payload));
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
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
        UNIT_ASSERT(writeResult->Get()->Record.GetPDiskNormalizedOccupancy() == expected);

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn));

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
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk.PBServiceId, 40, 2);
        auto write2 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds2, selector, lsn, NDDisk::TWriteInstruction(0));
        const TString payload2 = MakeData('Q', BlockSize);
        write2->AddPayloadThenChecksum(TRope(payload2));
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

    // Regression test for DirectBlockGroupIndex-aware persistent buffer keys: two direct block
    // groups of the SAME tablet, on the SAME generation, writing to the SAME lsn must be stored,
    // listed, read and erased as fully independent records (TPersistentBufferId /
    // TPersistentBufferRecordId now include DirectBlockGroupIndex). Before that change all of these
    // writes would have collided in a single "generation+lsn" slot.
    Y_UNIT_TEST(PersistentBufferDirectBlockGroupIndexSeparation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        const ui64 tabletId = 40;
        const ui32 generation = 1;
        const ui64 lsn = 10;
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        NDDisk::TQueryCredentials credsDbg0 = Connect(ctx, disk.PBServiceId, tabletId, generation, /*directBlockGroupIndex=*/0);
        NDDisk::TQueryCredentials credsDbg1 = Connect(ctx, disk.PBServiceId, tabletId, generation, /*directBlockGroupIndex=*/1);

        const TString payload0 = MakeData('A', BlockSize);
        auto write0 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(credsDbg0, selector, lsn, NDDisk::TWriteInstruction(0));
        write0->AddPayloadThenChecksum(TRope(payload0));
        SendToDDisk(ctx, disk.PBServiceId, write0.release());

        auto writeRaw0 = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(writeRaw0->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *writeRaw0, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult0 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult0, TReplyStatus::OK);

        const TString payload1 = MakeData('B', BlockSize);
        auto write1 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(credsDbg1, selector, lsn, NDDisk::TWriteInstruction(0));
        write1->AddPayloadThenChecksum(TRope(payload1));
        SendToDDisk(ctx, disk.PBServiceId, write1.release());

        auto writeRaw1 = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(writeRaw1->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *writeRaw1, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult1 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult1, TReplyStatus::OK);

        // Each direct block group must only see its own record via ListPersistentBuffer.
        auto listResultDbg0 = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(credsDbg0));
        AssertStatus(listResultDbg0, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg0->Get()->Record.RecordsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg0->Get()->Record.GetRecords(0).GetLsn(), lsn);

        auto listResultDbg1 = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(credsDbg1));
        AssertStatus(listResultDbg1, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg1->Get()->Record.RecordsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg1->Get()->Record.GetRecords(0).GetLsn(), lsn);

        // Reads must return the payload belonging to the requesting direct block group, not a
        // mixed/overwritten value.
        auto readResultDbg0 = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(credsDbg0, selector, lsn, 1, {true}));
        AssertStatus(readResultDbg0, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResultDbg0->Get()->GetPayload(0).ConvertToString(), payload0);

        auto readResultDbg1 = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(credsDbg1, selector, lsn, 1, {true}));
        AssertStatus(readResultDbg1, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResultDbg1->Get()->GetPayload(0).ConvertToString(), payload1);

        // Erasing DBG0's record must not affect DBG1's record for the same tablet/generation/lsn.
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(credsDbg0, lsn));
        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        auto missingReadDbg0 = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(credsDbg0, selector, lsn, 1, {true}));
        AssertStatus(missingReadDbg0, TReplyStatus::MISSING_RECORD);

        auto readResultDbg1AfterErase = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(credsDbg1, selector, lsn, 1, {true}));
        AssertStatus(readResultDbg1AfterErase, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResultDbg1AfterErase->Get()->GetPayload(0).ConvertToString(), payload1);

        auto listResultDbg0AfterErase = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(credsDbg0));
        AssertStatus(listResultDbg0AfterErase, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg0AfterErase->Get()->Record.RecordsSize(), 0);

        auto listResultDbg1AfterErase = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(credsDbg1));
        AssertStatus(listResultDbg1AfterErase, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(listResultDbg1AfterErase->Get()->Record.RecordsSize(), 1);
    }

    // TEvGetPersistentBufferInfo(DescribeTablets=true) must report separate TTabletInfo entries
    // per (TabletId, DirectBlockGroupIndex) pair, rather than merging every direct block group of a
    // tablet into one entry.
    Y_UNIT_TEST(PersistentBufferDirectBlockGroupIndexInfo) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        const ui64 tabletId = 41;
        const ui32 generation = 1;
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        NDDisk::TQueryCredentials credsDbg0 = Connect(ctx, disk.PBServiceId, tabletId, generation, /*directBlockGroupIndex=*/0);
        NDDisk::TQueryCredentials credsDbg2 = Connect(ctx, disk.PBServiceId, tabletId, generation, /*directBlockGroupIndex=*/2);

        auto doWrite = [&](const NDDisk::TQueryCredentials& creds, ui64 lsn, char fill) {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(MakeData(fill, BlockSize)));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(writeRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        };

        doWrite(credsDbg0, /*lsn=*/10, 'X');
        doWrite(credsDbg2, /*lsn=*/10, 'Y');

        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvGetPersistentBufferInfo(false, true));
        auto info = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);

        UNIT_ASSERT_VALUES_EQUAL(info->Get()->TabletInfos.size(), 2);
        bool foundDbg0 = false;
        bool foundDbg2 = false;
        for (const auto& ti : info->Get()->TabletInfos) {
            UNIT_ASSERT_VALUES_EQUAL(ti.TabletId, tabletId);
            if (ti.DirectBlockGroupIndex == 0) {
                foundDbg0 = true;
                UNIT_ASSERT_VALUES_EQUAL(ti.LsnsCount, 1u);
            } else if (ti.DirectBlockGroupIndex == 2) {
                foundDbg2 = true;
                UNIT_ASSERT_VALUES_EQUAL(ti.LsnsCount, 1u);
            } else {
                UNIT_FAIL("unexpected DirectBlockGroupIndex " << (ui32)ti.DirectBlockGroupIndex);
            }
        }
        UNIT_ASSERT(foundDbg0);
        UNIT_ASSERT(foundDbg2);
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
        write->AddPayloadThenChecksum(TRope(payload));
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
            write1->AddPayloadThenChecksum(TRope(payload));
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
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromDDisk(sourceId, 42, NDDisk::TBlockSelector(0, 0, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new TEvents::TEvUndelivered(NDDisk::TEv::EvRead, TEvents::TEvUndelivered::ReasonActorUnknown),
            0, readReq->Cookie), NodeId);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::ERROR);
    }

    Y_UNIT_TEST(SyncRejectsCorruptedSourcePayloadBeforeWrite) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(10, 2);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 51, 1);
        const ui32 srcPDiskId = 97;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(srcPDiskId, srcSlotId), 42,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        const TString payload = MakeData('C', BlockSize);
        const std::vector<ui64> badChecksums{
            NDDisk::CalculateRawChecksum(payload.data(), payload.size()) + 1};
        bool sawTargetPersistence = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && (ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType
                        || ev->GetTypeRewrite() == NPDisk::TEvLog::EventType)) {
                sawTargetPersistence = true;
            }
            return true;
        };
        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload), badChecksums),
            0, readReq->Cookie), NodeId);

        auto result = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        ctx.Runtime.FilterFunction = {};
        AssertStatus(result, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(result->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::CORRUPTED));
        UNIT_ASSERT_C(!sawTargetPersistence,
            "checksum-mismatched source data must not persist target data or integrity metadata");
    }

    Y_UNIT_TEST(ChecksumsDisabledSyncIgnoresSourceChecksums) {
        TTestContext ctx;
        NDDisk::TDDiskConfig config;
        config.EnableChecksums = false;
        const TDiskHandle disk =
            ctx.RegisterDDisk(10, 4, std::nullopt, config);
        ctx.BootstrapDDisk(disk, 4u << 20);
        NDDisk::TQueryCredentials creds =
            Connect(ctx, disk.ServiceId, 53, 1);

        const ui32 srcPDiskId = 95;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge =
            ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId),
            fakeSourceEdge);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(srcPDiskId, srcSlotId),
            42,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        auto readReq =
            ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        const TString payload = MakeData('I', BlockSize);
        const std::vector<ui64> badChecksums{
            NDDisk::CalculateRawChecksum(payload.data(), payload.size()) + 1};
        ctx.Runtime.Send(
            new IEventHandle(
                readReq->Sender,
                fakeSourceEdge,
                new NDDisk::TEvReadResult(
                    TReplyStatus::OK,
                    std::nullopt,
                    TRope(payload),
                    badChecksums),
                0,
                readReq->Cookie),
            NodeId);

        auto traffic =
            ctx.CollectAllocationTraffic(disk, true, 1, true);
        const auto increment =
            TTestContext::ParseChunkMapLog(*traffic.Increment->Get());
        UNIT_ASSERT(increment.GetChecksumsDisabled());
        UNIT_ASSERT(!increment.GetIncrement().GetDataChunk().HasExtentRef());
        UNIT_ASSERT(ctx.AutoServedIntegrityWriteChunks.empty());

        ctx.SendPDiskResponse(
            disk,
            *traffic.DataWrites.front(),
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);
        auto result = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(result, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(
                result->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(SyncRejectsSourceChecksumCountMismatchBeforeWrite) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(10, 3);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 52, 1);
        const ui32 srcPDiskId = 96;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);

        bool sawTargetPersistence = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && (ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType
                        || ev->GetTypeRewrite() == NPDisk::TEvLog::EventType)) {
                sawTargetPersistence = true;
            }
            return true;
        };
        const TString payload = MakeData('M', 2 * BlockSize);
        const auto validChecksums = MakeBlockChecksums(payload);
        for (const ui32 checksumCount : {1u, 3u}) {
            auto sync = std::make_unique<NDDisk::TEvSync>(creds);
            sync->AddSegmentFromDDisk(
                MakeSyncSourceId(srcPDiskId, srcSlotId), 42,
                NDDisk::TBlockSelector(0, 0, 2 * BlockSize));
            SendToDDisk(ctx, disk.ServiceId, sync.release());
            auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});

            std::vector<ui64> checksums = validChecksums;
            checksums.resize(checksumCount, 0);
            ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
                new NDDisk::TEvReadResult(
                    TReplyStatus::OK, std::nullopt, TRope(payload), checksums),
                0, readReq->Cookie), NodeId);

            auto result = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
            AssertStatus(result, TReplyStatus::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.SegmentResultsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(result->Get()->Record.GetSegmentResults(0).GetStatus()),
                static_cast<int>(TReplyStatus::INCORRECT_REQUEST));
        }
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_C(!sawTargetPersistence,
            "source checksum count mismatch must not persist target data or integrity metadata");
    }

    Y_UNIT_TEST(LateOutdatedSyncReadResultDoesNotLogUnknownSync) {
        TStringStream log;
        TTestContext ctx;
        ctx.Runtime.LogStream = &log;
        ctx.Runtime.SetLogPriority(NKikimrServices::BS_DDISK, NLog::PRI_ERROR);

        const TDiskHandle disk = ctx.CreateDDisk(11, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 99;
        const ui32 srcSlotId = 1;
        TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId = MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId);
        ctx.Runtime.RegisterService(fakeSourceServiceId, fakeSourceEdge);
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        auto sendSync = [&] {
            auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
            syncEv->AddSegmentFromDDisk(sourceId, 42, NDDisk::TBlockSelector(7, 0, BlockSize));
            SendToDDisk(ctx, disk.ServiceId, syncEv.release());
        };

        sendSync();
        auto staleReadReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(staleReadReq->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        sendSync();

        std::unique_ptr<IEventHandle> freshReadReq;
        std::unique_ptr<IEventHandle> staleSyncResultRaw;
        for (ui32 i = 0; i < 2; ++i) {
            auto ev = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, fakeSourceEdge});
            if (ev->GetTypeRewrite() == static_cast<ui32>(NDDisk::TEv::EvRead)) {
                freshReadReq = std::move(ev);
            } else if (ev->GetTypeRewrite() == NDDisk::TEvSyncResult::EventType) {
                staleSyncResultRaw = std::move(ev);
            } else {
                UNIT_FAIL("unexpected event: " << ev->ToString());
            }
        }

        UNIT_ASSERT(freshReadReq);
        UNIT_ASSERT(staleSyncResultRaw);

        auto staleSyncResult = std::unique_ptr<TEventHandle<NDDisk::TEvSyncResult>>(
            reinterpret_cast<TEventHandle<NDDisk::TEvSyncResult>*>(staleSyncResultRaw.release()));
        AssertStatus(staleSyncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(staleSyncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(staleSyncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OUTDATED));

        const TString stalePayload = MakeData('O', BlockSize);
        bool sawTargetPersistence = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && (ev->GetTypeRewrite() == NPDisk::TEvChunkWriteRaw::EventType
                        || ev->GetTypeRewrite() == NPDisk::TEvLog::EventType)) {
                sawTargetPersistence = true;
            }
            return true;
        };
        ctx.Runtime.Send(new IEventHandle(staleReadReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(stalePayload),
                MakeBlockChecksums(stalePayload)),
            0, staleReadReq->Cookie), NodeId);

        auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, disk.ServiceId, new NDDisk::TEvConnect(creds));
        ctx.Runtime.FilterFunction = {};
        AssertStatus(connectResult, TReplyStatus::OK);

        UNIT_ASSERT_C(!sawTargetPersistence,
            "a late source result made stale by an overlapping sync must not persist metadata");
        UNIT_ASSERT_C(!log.Str().Contains("unknown sync for cookie"), log.Str());
    }

    Y_UNIT_TEST(UnknownSyncReadResultLogsUnknownSync) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(13, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        TStringStream log;
        ctx.Runtime.LogStream = &log;
        ctx.Runtime.SetLogPriority(NKikimrServices::BS_DDISK, NLog::PRI_ERROR);

        const ui64 unknownCookie = 424242;
        const TString payload = MakeData('U', BlockSize);
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvReadResult(TReplyStatus::OK, std::nullopt, TRope(payload)),
            unknownCookie);

        auto connectResult = SendToDDiskAndWait<NDDisk::TEvConnectResult>(ctx, disk.ServiceId, new NDDisk::TEvConnect(creds));
        AssertStatus(connectResult, TReplyStatus::OK);

        UNIT_ASSERT_C(log.Str().Contains("unknown sync for cookie"), log.Str());
    }

    Y_UNIT_TEST(SyncViaFakeSource) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(11, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 99;
        const ui32 srcSlotId = 1;
        TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId = MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId);
        ctx.Runtime.RegisterService(fakeSourceServiceId, fakeSourceEdge);
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        const TString payload = MakeData('S', BlockSize);
        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromDDisk(sourceId, 42, NDDisk::TBlockSelector(7, 0, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(readReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                readRecord.GetCredentials().GetInternal().GetTabletId(),
                50
            );
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }

        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload), MakeBlockChecksums(payload)),
            0, readReq->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(DDiskToDDiskSyncPreservesPureChecksums) {
        TTestContext ctx;
        const TDiskHandle source = ctx.CreateDDisk(70, 1);
        const TDiskHandle destination = ctx.CreateDDisk(71, 1);
        NDDisk::TQueryCredentials sourceCreds =
            Connect(ctx, source.ServiceId, 250, 1);
        NDDisk::TQueryCredentials destinationCreds =
            Connect(ctx, destination.ServiceId, 250, 1);
        const TString payload =
            MakeData('A', BlockSize) + MakeData('B', BlockSize);
        const auto expectedChecksums = MakeBlockChecksums(payload);

        auto sourceWrite = DoWriteWithChunkAllocation(
            ctx, source, MakeWrite(sourceCreds, 3, 0, payload),
            source.FirstChunkId + PersistentBufferInitChunks, 0, payload, true, true);
        AssertStatus(sourceWrite.WriteResult, TReplyStatus::OK);

        auto sync = std::make_unique<NDDisk::TEvSync>(destinationCreds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(source.PDiskId, source.SlotId),
            *sourceCreds.DDiskInstanceGuid,
            NDDisk::TBlockSelector(3, 0, payload.size()));
        SendToDDisk(ctx, destination.ServiceId, sync.release());

        auto sourceRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(source);
        UNIT_ASSERT_VALUES_EQUAL(sourceRead->Get()->ChunkIdx, sourceWrite.ChunkIdx);
        ctx.SendPDiskResponse(source, *sourceRead,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));

        auto allocation = ctx.CollectAllocationTraffic(destination, true, 1);
        ctx.SendPDiskResponse(destination, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(destination, *allocation.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvSyncResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, destination.ServiceId,
            new NDDisk::TEvRead(destinationCreds, {3, 0, static_cast<ui32>(payload.size())}, {true}));
        auto destinationRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(destination);
        ctx.SendPDiskResponse(destination, *destinationRead,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
        UNIT_ASSERT_VALUES_EQUAL(
            readResult->Get()->Record.ChecksumsSize(), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }
    }

    Y_UNIT_TEST(PersistentBufferToDDiskSyncPreservesPureChecksums) {
        TTestContext ctx;
        const TDiskHandle source = ctx.CreateDDisk(72, 1);
        const TDiskHandle destination = ctx.CreateDDisk(73, 1);
        NDDisk::TQueryCredentials sourceCreds =
            Connect(ctx, source.PBServiceId, 251, 1);
        NDDisk::TQueryCredentials destinationCreds =
            Connect(ctx, destination.ServiceId, 251, 1);
        constexpr ui64 Lsn = 10;
        const TString payload =
            MakeData('P', BlockSize) + MakeData('Q', BlockSize);
        const NDDisk::TBlockSelector selector{
            4, 2 * BlockSize, static_cast<ui32>(payload.size())};
        const auto expectedChecksums = MakeBlockChecksums(payload);

        auto sourceWrite = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
            sourceCreds, selector, Lsn, NDDisk::TWriteInstruction(0));
        sourceWrite->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, source.PBServiceId, sourceWrite.release());
        auto sourceWriteRaw =
            ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(source);
        ctx.SendPDiskResponse(source, *sourceWriteRaw,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(
            WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx),
            TReplyStatus::OK);

        auto sync = std::make_unique<NDDisk::TEvSync>(destinationCreds);
        sync->AddSegmentFromPB(
            MakeSyncSourceId(source.PDiskId, source.SlotId),
            *sourceCreds.DDiskInstanceGuid, selector, Lsn, sourceCreds.Generation);
        SendToDDisk(ctx, destination.ServiceId, sync.release());

        auto allocation = ctx.CollectAllocationTraffic(destination, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            allocation.DataWrites[0]->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(destination, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(destination, *allocation.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvSyncResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, destination.ServiceId,
            new NDDisk::TEvRead(destinationCreds, selector, {true}));
        auto destinationRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(destination);
        ctx.SendPDiskResponse(destination, *destinationRead,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
        UNIT_ASSERT_VALUES_EQUAL(
            readResult->Get()->Record.ChecksumsSize(), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }
    }

    Y_UNIT_TEST(SyncSlicesChecksumsAcrossSegmentsAndIntegrityPair) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(74, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 252, 1);
        constexpr ui32 SourcePDiskId = 93;
        const TActorId sourceEdge =
            ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, SourcePDiskId, 1), sourceEdge);

        const ui32 firstOffset =
            (NDDisk::ChecksumsPerIntegrityBlock - 1) * BlockSize;
        const TString firstPayload =
            MakeData('A', BlockSize) + MakeData('B', BlockSize);
        const TString secondPayload =
            MakeData('C', BlockSize) + MakeData('D', BlockSize);
        const TString allPayload = firstPayload + secondPayload;
        const auto expectedChecksums = MakeBlockChecksums(allPayload);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 42,
            NDDisk::TBlockSelector(
                6, firstOffset, static_cast<ui32>(firstPayload.size())));
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 42,
            NDDisk::TBlockSelector(
                6, firstOffset + firstPayload.size(),
                static_cast<ui32>(secondPayload.size())));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        for (const TString* payload : {&firstPayload, &secondPayload}) {
            auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({sourceEdge});
            ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, sourceEdge,
                new NDDisk::TEvReadResult(
                    TReplyStatus::OK, std::nullopt, TRope(*payload),
                    MakeBlockChecksums(*payload)),
                0, sourceRead->Cookie), NodeId);
        }

        auto allocation = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            allocation.DataWrites[0]->Get()->Offset, firstOffset);
        UNIT_ASSERT_VALUES_EQUAL(
            allocation.DataWrites[0]->Get()->Data.ConvertToString(), firstPayload);
        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *allocation.Increment);

        auto secondWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(
            secondWrite->Get()->Offset, firstOffset + firstPayload.size());
        UNIT_ASSERT_VALUES_EQUAL(
            secondWrite->Get()->Data.ConvertToString(), secondPayload);
        ctx.SendPDiskResponse(disk, *secondWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(WaitFromDDisk<NDDisk::TEvSyncResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {6, firstOffset, static_cast<ui32>(allPayload.size())},
                {true}));
        auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        ctx.SendPDiskResponse(disk, *dataRead,
            new NPDisk::TEvChunkReadRawResult(TRope(allPayload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(
            readResult->Get()->Record.ChecksumsSize(), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }
    }

    Y_UNIT_TEST(SyncReplyWaitsForDestinationDataAndIntegrity) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(76, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 254, 1);
        const TString initialPayload = MakeData('A', BlockSize);
        auto initial = DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(creds, 0, 0, initialPayload),
            disk.FirstChunkId + PersistentBufferInitChunks,
            0, initialPayload, true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        constexpr ui32 SourcePDiskId = 92;
        const TActorId sourceEdge =
            ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, SourcePDiskId, 1), sourceEdge);
        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 42,
            NDDisk::TBlockSelector(0, BlockSize, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({sourceEdge});
        const TString sourcePayload = MakeData('S', BlockSize);
        ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, sourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(sourcePayload),
                MakeBlockChecksums(sourcePayload)),
            0, sourceRead->Cookie), NodeId);

        auto write1 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto write2 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto* dataWrite =
            write1->Get()->ChunkIdx == initial.ChunkIdx ? write1.get() : write2.get();
        auto* integrityWrite =
            write1->Get()->ChunkIdx == initial.ChunkIdx ? write2.get() : write1.get();

        bool sawSyncReply = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDDisk::TEvSyncResult::EventType
                    && ev->GetRecipientRewrite() == ctx.Edge) {
                sawSyncReply = true;
            }
            return true;
        };
        ctx.SendPDiskResponse(disk, *integrityWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertNoClientReplyBeforeSentinel(
            ctx, "sync reply must wait for destination data after integrity is durable");
        UNIT_ASSERT(!sawSyncReply);

        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto result = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT(sawSyncReply);
        AssertStatus(result, TReplyStatus::OK);
    }

    Y_UNIT_TEST(SyncReplyWaitsForCombinedIncrement) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(28, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 208, 1);

        const ui32 srcPDiskId = 91;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(sourceId, 42,
            NDDisk::TBlockSelector(7, 0, BlockSize));
        sync->AddSegmentFromDDisk(sourceId, 42,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        auto firstRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        auto secondRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(firstRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));
        UNIT_ASSERT_VALUES_EQUAL(secondRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        const TString payload = MakeData('G', BlockSize);
        ctx.Runtime.Send(new IEventHandle(firstRead->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload), MakeBlockChecksums(payload)),
            0, firstRead->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // The second source failure completes the sync, but its reply must remain parked until
        // the combined data/integrity allocation increment is durable.
        ctx.Runtime.Send(new IEventHandle(secondRead->Sender, fakeSourceEdge,
            new TEvents::TEvUndelivered(
                NDDisk::TEv::EvRead, TEvents::TEvUndelivered::ReasonActorUnknown),
            0, secondRead->Cookie), NodeId);

        const TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        auto sentinel = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, sentinelEdge});
        UNIT_ASSERT_VALUES_EQUAL_C(sentinel->Recipient, sentinelEdge,
            "TEvSyncResult must wait for the combined increment to commit");

        ctx.ReplyLog(disk, *traffic.Increment);
        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(1).GetStatus()),
            static_cast<int>(TReplyStatus::ERROR));
    }

    Y_UNIT_TEST(OutdatedSyncReplyWaitsForCommit) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(29, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 209, 1);

        const ui32 srcPDiskId = 90;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        auto firstSync = std::make_unique<NDDisk::TEvSync>(creds);
        firstSync->AddSegmentFromDDisk(sourceId, 42,
            NDDisk::TBlockSelector(7, 0, BlockSize));
        firstSync->AddSegmentFromDDisk(sourceId, 42,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, firstSync.release());

        auto completedRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        auto staleRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(completedRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));
        UNIT_ASSERT_VALUES_EQUAL(staleRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        const TString payload = MakeData('O', BlockSize);
        ctx.Runtime.Send(new IEventHandle(completedRead->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload), MakeBlockChecksums(payload)),
            0, completedRead->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // A newer overlapping sync outdates the only request still outstanding in firstSync.
        // Its result must be parked because the first segment wrote into the allocating chunk.
        auto newerSync = std::make_unique<NDDisk::TEvSync>(creds);
        newerSync->AddSegmentFromDDisk(sourceId, 43,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, newerSync.release());
        auto newerRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(newerRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        // A late undelivered notification for the already-OUTDATED request must be ignored:
        // it must neither double-decrement RequestsInFlight nor replace OUTDATED with ERROR.
        ctx.Runtime.Send(new IEventHandle(staleRead->Sender, fakeSourceEdge,
            new TEvents::TEvUndelivered(
                NDDisk::TEv::EvRead, TEvents::TEvUndelivered::ReasonActorUnknown),
            0, staleRead->Cookie), NodeId);

        const TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        auto sentinel = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, sentinelEdge});
        UNIT_ASSERT_VALUES_EQUAL_C(sentinel->Recipient, sentinelEdge,
            "outdated TEvSyncResult must wait for the combined increment to commit");

        ctx.ReplyLog(disk, *traffic.Increment);
        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(1).GetStatus()),
            static_cast<int>(TReplyStatus::OUTDATED));
    }

    Y_UNIT_TEST(SyncReadsFromMultipleDDiskSources) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(13, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId1 = 97;
        const ui32 srcSlotId1 = 1;
        TActorId fakeSourceEdge1 = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId1 = MakeBlobStorageDDiskId(NodeId, srcPDiskId1, srcSlotId1);
        ctx.Runtime.RegisterService(fakeSourceServiceId1, fakeSourceEdge1);
        const auto sourceId1 = MakeSyncSourceId(srcPDiskId1, srcSlotId1);

        const ui32 srcPDiskId2 = 96;
        const ui32 srcSlotId2 = 1;
        TActorId fakeSourceEdge2 = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeSourceServiceId2 = MakeBlobStorageDDiskId(NodeId, srcPDiskId2, srcSlotId2);
        ctx.Runtime.RegisterService(fakeSourceServiceId2, fakeSourceEdge2);
        const auto sourceId2 = MakeSyncSourceId(srcPDiskId2, srcSlotId2);

        const TString payload1 = MakeData('A', BlockSize);
        const TString payload2 = MakeData('B', BlockSize);

        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromDDisk(sourceId1, 42, NDDisk::TBlockSelector(7, 0, BlockSize));
        syncEv->AddSegmentFromDDisk(
            sourceId2,
            43,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize)
        );

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq1 = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge1});
        UNIT_ASSERT_VALUES_EQUAL(readReq1->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(readReq1.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }
        ctx.Runtime.Send(new IEventHandle(readReq1->Sender, fakeSourceEdge1,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload1), MakeBlockChecksums(payload1)),
            0, readReq1->Cookie), NodeId);

        auto readReq2 = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge2});
        UNIT_ASSERT_VALUES_EQUAL(readReq2->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(readReq2.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }
        ctx.Runtime.Send(new IEventHandle(readReq2->Sender, fakeSourceEdge2,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(payload2), MakeBlockChecksums(payload2)),
            0, readReq2->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), payload1);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto secondWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Offset, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Data.ConvertToString(), payload2);
        ctx.SendPDiskResponse(disk, *secondWrite, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(1).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(SyncReadsFromMixedSources) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(14, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPBufferPDiskId = 95;
        const ui32 srcPBufferSlotId = 1;
        TActorId fakePBufferSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakePBufferSourceServiceId = MakeBlobStoragePersistentBufferId(
            NodeId,
            srcPBufferPDiskId,
            srcPBufferSlotId);
        ctx.Runtime.RegisterService(fakePBufferSourceServiceId, fakePBufferSourceEdge);
        const auto pbufferSourceId = MakeSyncSourceId(srcPBufferPDiskId, srcPBufferSlotId);

        const ui32 srcDDiskPDiskId = 94;
        const ui32 srcDDiskSlotId = 1;
        TActorId fakeDDiskSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeDDiskSourceServiceId = MakeBlobStorageDDiskId(
            NodeId,
            srcDDiskPDiskId,
            srcDDiskSlotId);
        ctx.Runtime.RegisterService(fakeDDiskSourceServiceId, fakeDDiskSourceEdge);
        const auto ddiskSourceId = MakeSyncSourceId(srcDDiskPDiskId, srcDDiskSlotId);

        const TString pbufferPayload = MakeData('P', BlockSize);
        const TString ddiskPayload = MakeData('D', BlockSize);

        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromPB(
            pbufferSourceId,
            42,
            NDDisk::TBlockSelector(7, 0, BlockSize),
            10,
            1);
        syncEv->AddSegmentFromDDisk(
            ddiskSourceId,
            43,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto pbufferReadReq = ctx.Runtime.WaitForEdgeActorEvent({fakePBufferSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(
            pbufferReadReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvReadPersistentBuffer));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvReadPersistentBuffer>*>(
                pbufferReadReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetLsn(), 10u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetGeneration(), 1u);
        }
        ctx.Runtime.Send(new IEventHandle(pbufferReadReq->Sender, fakePBufferSourceEdge,
            new NDDisk::TEvReadPersistentBufferResult(TReplyStatus::OK, std::nullopt,
                7, 0, BlockSize, TRope(pbufferPayload), MakeBlockChecksums(pbufferPayload)),
            0, pbufferReadReq->Cookie), NodeId);

        auto ddiskReadReq = ctx.Runtime.WaitForEdgeActorEvent({fakeDDiskSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(
            ddiskReadReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvRead));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(
                ddiskReadReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }
        ctx.Runtime.Send(new IEventHandle(ddiskReadReq->Sender, fakeDDiskSourceEdge,
            new NDDisk::TEvReadResult(TReplyStatus::OK, std::nullopt,
                TRope(ddiskPayload), MakeBlockChecksums(ddiskPayload)),
            0, ddiskReadReq->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), pbufferPayload);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto secondWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Offset, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Data.ConvertToString(), ddiskPayload);
        ctx.SendPDiskResponse(disk, *secondWrite, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(1).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(SyncReadsFromMixedSegmentKindsInSingleSource) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(15, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 50, 1);

        const ui32 srcPDiskId = 96;
        const ui32 srcSlotId = 1;

        TActorId fakePBufferSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakePBufferSourceServiceId = MakeBlobStoragePersistentBufferId(
            NodeId,
            srcPDiskId,
            srcSlotId);
        ctx.Runtime.RegisterService(fakePBufferSourceServiceId, fakePBufferSourceEdge);
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        TActorId fakeDDiskSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        TActorId fakeDDiskSourceServiceId = MakeBlobStorageDDiskId(
            NodeId,
            srcPDiskId,
            srcSlotId);
        ctx.Runtime.RegisterService(fakeDDiskSourceServiceId, fakeDDiskSourceEdge);

        const TString pbufferPayload = MakeData('P', BlockSize);
        const TString ddiskPayload = MakeData('D', BlockSize);

        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromPB(
            sourceId,
            42,
            NDDisk::TBlockSelector(7, 0, BlockSize),
            10,
            1);
        syncEv->AddSegmentFromDDisk(
            sourceId,
            42,
            NDDisk::TBlockSelector(7, BlockSize, BlockSize));

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto pbufferReadReq = ctx.Runtime.WaitForEdgeActorEvent({fakePBufferSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(
            pbufferReadReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvReadPersistentBuffer));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvReadPersistentBuffer>*>(
                pbufferReadReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetLsn(), 10u);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetGeneration(), 1u);
        }
        ctx.Runtime.Send(new IEventHandle(pbufferReadReq->Sender, fakePBufferSourceEdge,
            new NDDisk::TEvReadPersistentBufferResult(TReplyStatus::OK, std::nullopt,
                7, 0, BlockSize, TRope(pbufferPayload), MakeBlockChecksums(pbufferPayload)),
            0, pbufferReadReq->Cookie), NodeId);

        auto ddiskReadReq = ctx.Runtime.WaitForEdgeActorEvent({fakeDDiskSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(
            ddiskReadReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvRead));
        {
            auto* readEv = reinterpret_cast<TEventHandle<NDDisk::TEvRead>*>(
                ddiskReadReq.get());
            const auto& readRecord = readEv->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetVChunkIndex(), 7);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetOffsetInBytes(), BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(readRecord.GetSelector().GetSize(), BlockSize);
        }
        ctx.Runtime.Send(new IEventHandle(ddiskReadReq->Sender, fakeDDiskSourceEdge,
            new NDDisk::TEvReadResult(TReplyStatus::OK, std::nullopt,
                TRope(ddiskPayload), MakeBlockChecksums(ddiskPayload)),
            0, ddiskReadReq->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), pbufferPayload);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto secondWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Offset, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(secondWrite->Get()->Data.ConvertToString(), ddiskPayload);
        ctx.SendPDiskResponse(disk, *secondWrite, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(1).GetStatus()),
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
        const auto sourceId = MakeSyncSourceId(srcPDiskId, srcSlotId);

        const TString payload = MakeData('P', BlockSize);
        auto syncEv = std::make_unique<NDDisk::TEvSync>(creds);
        syncEv->AddSegmentFromPB(
            sourceId,
            42,
            NDDisk::TBlockSelector(5, 0, BlockSize),
            10,
            1);

        SendToDDisk(ctx, disk.ServiceId, syncEv.release());

        auto readReq = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(readReq->GetTypeRewrite(),
            static_cast<ui32>(NDDisk::TEv::EvReadPersistentBuffer));
        ctx.Runtime.Send(new IEventHandle(readReq->Sender, fakeSourceEdge,
            new NDDisk::TEvReadPersistentBufferResult(TReplyStatus::OK, std::nullopt,
                5, 0, BlockSize, TRope(payload), MakeBlockChecksums(payload)),
            0, readReq->Cookie), NodeId);

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->Data.ConvertToString(), payload);
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::OK));
    }

    Y_UNIT_TEST(PersistentBufferOverfill) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk.PBServiceId, 40, 1);
        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk.PBServiceId, 60, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('P', BlockSize * 128);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize * 128};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn + 1, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OVERFILL);

        write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds2, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());
        pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(PersistentBufferReadSequential) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const ui32 size = BlockSize * 100;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        { // Overfill inmemory cache - pop previous lsn data
            NDDisk::TQueryCredentials creds2 = Connect(ctx, disk.PBServiceId, 50, 1);
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds2, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        {
            for (auto i : xrange(3)) {
                const NDDisk::TBlockSelector readSelector{3, BlockSize * (i + 1), BlockSize * (i + 10)};
                SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds, readSelector, lsn, 1, {true}), i);
            }

            auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(payload)));

            for (auto _ : xrange(3)) {
                auto readResult = WaitFromDDisk<NDDisk::TEvReadPersistentBufferResult>(ctx);
                AssertStatus(readResult, TReplyStatus::OK);
                auto actual = readResult->Get()->GetPayload(0).ConvertToString();
                auto expected = payload.substr(BlockSize * (readResult->Cookie + 1), BlockSize * (readResult->Cookie + 10));
                UNIT_ASSERT_VALUES_EQUAL(actual, expected);
            }
        }
    }

    void TestPDiskErrorStopsDDisk(NKikimrProto::EReplyStatus errorStatus) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(20, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 100, 1);

        const TString payload = MakeData('X', BlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(payload));
        SendToDDisk(ctx, disk.ServiceId, write.release());

        // A first-time write triggers a chunk-map snapshot (and, in parallel, formatting I/O,
        // a reserve refill and the data write). Inject the error on the snapshot so DDisk
        // enters the PDisk-session termination state before any increment is issued.
        auto logSnapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);

        auto logReply = std::make_unique<NPDisk::TEvLogResult>(errorStatus, 0, "test injected error", 0);
        logReply->Results.emplace_back(logSnapshot->Get()->Lsn, logSnapshot->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *logSnapshot, logReply.release());

        // DDisk should be in StateFuncTerminate now (not crashed).
        // Verify it silently drops client requests.
        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvConnect(creds));

        TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);

        for (;;) {
            auto ev = ctx.Runtime.WaitForEdgeActorEvent(ctx.ClientWaitEdges({sentinelEdge}));
            if (ctx.ConsumeUnsolicitedPDiskEvent(ev)) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
                "DDisk should not respond to client requests after PDisk "
                << NKikimrProto::EReplyStatus_Name(errorStatus));
            break;
        }
    }

    Y_UNIT_TEST(PDiskCorruptedStopsDDisk) {
        TestPDiskErrorStopsDDisk(NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(PDiskOutOfSpaceStopsDDisk) {
        TestPDiskErrorStopsDDisk(NKikimrProto::OUT_OF_SPACE);
    }

    Y_UNIT_TEST(PersistentBufferWriteDuplicatesInflight) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const ui32 size = BlockSize * 100;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};
        {
            for (ui32 _ : xrange(10)) {
                auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
                write->AddPayloadThenChecksum(TRope(payload));
                SendToDDisk(ctx, disk.PBServiceId, write.release());
            }

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            for (ui32 _ : xrange(10)) {
                auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
                AssertStatus(writeResult, TReplyStatus::OK);
            }
        }
    }

    Y_UNIT_TEST(PersistentBufferWriteDuplicatesInflightBadData) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const ui32 size = BlockSize * 100;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));

        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
        {
            // Invalid data
            TString badPayload = payload;
            badPayload[badPayload.size() - 1000] = 123;
            auto badWrite = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            badWrite->AddPayloadThenChecksum(TRope(badPayload));
            SendToDDisk(ctx, disk.PBServiceId, badWrite.release());

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
        {
            // invalid VChunk
            const NDDisk::TBlockSelector badSelector{4, 0, size};
            auto badWrite = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, badSelector, lsn, NDDisk::TWriteInstruction(0));
            badWrite->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, badWrite.release());

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(PersistentBufferWriteDuplicates) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const ui32 size = BlockSize * 100;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
        for (ui32 _ : xrange(10)) {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
        {
            // invalid VChunk
            const NDDisk::TBlockSelector badSelector{4, 0, size};
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, badSelector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
        {
            // Invalid data
            TString badPayload = payload;
            badPayload[badPayload.size() - 1000] = 123;
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(badPayload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::INCORRECT_REQUEST);
        }
    }

    Y_UNIT_TEST(PersistentBufferWriteBeforeBarrier) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        ui64 lsn = 1;
        const ui32 size = BlockSize;
        TString payload = NUnitTest::RandomString(size);
        const NDDisk::TBlockSelector selector{3, 0, size};
        for (auto _ : xrange(10)) {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn++, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() == size + BlockSize);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, 5));

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        // write before barrier error
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, 3, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OUTDATED);
    }

    Y_UNIT_TEST(PersistentBufferPendingQueueOverfill) {
        TTestContext ctx;
        // Disable proactive chunk preallocation (PreallocateFreeSpaceThresholdPercent = 0):
        // this test exercises the reactive allocation path that fires only when
        // the buffer is completely exhausted.  With the default threshold the
        // proactive path would allocate a chunk around write 916 and break the
        // strict event sequence expected below.
        NDDisk::TPersistentBufferFormat fmt;
        fmt.MaxChunks = 256;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 512 * 1024;
        fmt.PreallocateFreeSpaceThresholdPercent = 0;
        const TDiskHandle disk = ctx.CreateDDisk(6, 1, fmt);
        std::unique_ptr<TEventHandle<NPDisk::TEvLog>> log;

        for (ui32 i : xrange(1015 + 1024 + 15)) {
            NDDisk::TQueryCredentials creds1 = Connect(ctx, disk.PBServiceId, i + 1, 1);
            const ui64 lsn = 10;
            const TString payload = MakeData('P', BlockSize * 128);
            const NDDisk::TBlockSelector selector{3, 0, BlockSize * 128};

            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            if (i < 1016) {
                auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
                UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
                ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

                auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
                AssertStatus(writeResult, TReplyStatus::OK);
            } else if (i == 1016) {
                log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);

                auto reserve = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
                UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, 1);
                auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
                reserveReply->ChunkIds.push_back(disk.FirstChunkId + PersistentBufferInitChunks + 5);
                ctx.SendPDiskResponse(disk, *reserve, reserveReply.release());
            }
            else if (i > 1015 + 1024) {
                auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
                AssertStatus(writeResult, TReplyStatus::OVERLOADED);
            }
        }

        auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *log, logReply.release());

        for (ui32 chunkIdx : xrange(4)) { // we need 4 more chunks to process pending queue
            for (ui32 _ : xrange(254)) {
                auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
                UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
                ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            }
            auto log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);

            auto reserve = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
            UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, 1);
            auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + PersistentBufferInitChunks + 10 + chunkIdx); // some new chunk
            ctx.SendPDiskResponse(disk, *reserve, reserveReply.release());

            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            for (ui32 _ : xrange(254)) {
                auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
                AssertStatus(writeResult, TReplyStatus::OK);
            }
            ctx.SendPDiskResponse(disk, *log, logReply.release());
        }
        std::unique_ptr<NDDisk::TEvGetPersistentBufferInfo> ev(new NDDisk::TEvGetPersistentBufferInfo(false, false));
        SendToDDisk(ctx, disk.PBServiceId, ev.release());
        auto res = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->PendingEvents, 8);
    }

    // Helper: create a DDisk instance that simulates a restart where the PB chunks from a
    // previous instance are passed via StartingPoints.  The new instance will have a different
    // PersistentBufferUniqueId (oldUniqueId + 1), so all checksums written by the old instance
    // will fail verification and the records will be discarded.
    //
    // `preExistingChunkIds` – chunk IDs that were owned by the previous PB instance.
    // `oldUniqueId`         – the UniqueId that was used by the previous instance.
    // `chunkData`           – maps chunkId -> raw bytes (ChunkSize) to return during restore reads.
    TDiskHandle CreateDDiskWithRestoredChunkData(TTestContext& ctx, ui32 pdiskId, ui32 slotId,
            const std::vector<ui32>& preExistingChunkIds, ui64 oldUniqueId,
            const std::unordered_map<ui32, TString>& chunkData) {
        const TActorId pdiskEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        const TActorId pdiskServiceId = MakeBlobStoragePDiskID(NodeId, pdiskId);
        ctx.Runtime.RegisterService(pdiskServiceId, pdiskEdge);

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
        const TActorId ddiskActor = ctx.Runtime.Register(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo,
            std::move(pbFormat), NDDisk::TDDiskConfig{}, ctx.Counters),
            NodeId);
        const TActorId ddiskServiceId = MakeBlobStorageDDiskId(NodeId, pdiskId, slotId);
        const TActorId pbServiceId = MakeBlobStoragePersistentBufferId(NodeId, pdiskId, slotId);
        ctx.Runtime.RegisterService(ddiskServiceId, ddiskActor);

        TDiskHandle disk{
            ddiskServiceId,
            pbServiceId,
            pdiskEdge,
            pdiskId,
            slotId,
            100000 + pdiskId * 1000,
            true};

        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;

        // ── Step 1: TEvYardInit → reply with StartingPoints containing old PB chunk map ──
        // The new instance will read UniqueId from StartingPoints and use it for checksum
        // verification.  We pass oldUniqueId so the PB actor verifies with the same key
        // that was used to write the data.  Since the data was written with oldUniqueId,
        // the checksums WILL match — but we want them to FAIL.
        //
        // The correct approach: pass a *different* UniqueId (oldUniqueId + 1) so that
        // checksum verification fails and stale records are discarded.
        // However, the UniqueId stored in StartingPoints is what the PB actor uses for
        // verification (see ddisk_actor_boot.cpp: PersistentBufferUniqueId = chunkMap.GetUniqueId()).
        // So we pass (oldUniqueId + 1) to simulate a fresh restart with a new UniqueId.
        auto init = ctx.WaitPDiskRequest<NPDisk::TEvYardInit>(disk);
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

        // Populate StartingPoints with the old PB chunk map, but with a *different* UniqueId.
        // This causes the PB actor to own the pre-existing chunks (triggering restore) but
        // verify checksums with a different key → all stale sectors fail → records discarded.
        {
            NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord pbChunkMap;
            for (ui32 chunkIdx : preExistingChunkIds) {
                pbChunkMap.AddChunkIdxs(chunkIdx);
            }
            // Use a different UniqueId than what was used to write the data.
            // The old data was written with oldUniqueId; we verify with (oldUniqueId + 1).
            pbChunkMap.SetUniqueId(oldUniqueId + 1);

            TString pbChunkMapData;
            const bool serializeOk = pbChunkMap.SerializeToString(&pbChunkMapData);
            Y_ABORT_UNLESS(serializeOk);
            initReply->StartingPoints[TLogSignature::SignaturePersistentBufferChunkMap] =
                NPDisk::TLogRecord(TLogSignature::SignaturePersistentBufferChunkMap,
                                   TRcBuf(pbChunkMapData), 1 /*lsn*/);
        }

        ctx.SendPDiskResponse(disk, *init, initReply.release());

        // ── Step 2: TEvReadLog → end-of-log (no increments) ──────────────────────
        auto readLog = ctx.WaitPDiskRequest<NPDisk::TEvReadLog>(disk);
        auto readLogReply = std::make_unique<NPDisk::TEvReadLogResult>(
            NKikimrProto::OK,
            readLog->Get()->Position,
            readLog->Get()->Position,
            true,
            0,
            "",
            Owner);
        ctx.SendPDiskResponse(disk, *readLog, readLogReply.release());

        // ── Step 3: TEvChunkReadRaw × preExistingChunkIds.size() ─────────────────
        // The PB actor issues a read for each pre-existing chunk to restore its contents.
        // Since the PB chunks already exist (from StartingPoints), no TEvChunkReserve is
        // sent — the PB actor goes directly to the restore path.
        // We return the stale data; checksum verification will fail (different UniqueId)
        // and the records will be silently discarded.
        for (ui32 i = 0; i < preExistingChunkIds.size(); ++i) {
            auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            const ui32 chunkIdx = readRaw->Get()->ChunkIdx;
            auto it = chunkData.find(chunkIdx);
            TString data;
            if (it != chunkData.end()) {
                data = it->second;
            } else {
                data = TString(TTestContext::ChunkSize, '\0');
            }
            ctx.SendPDiskResponse(disk, *readRaw, new NPDisk::TEvChunkReadRawResult(TRope(data)));
        }

        return disk;
    }

    // Test: a new PersistentBuffer instance must NOT restore records written by a previous
    // instance (different PersistentBufferUniqueId) even when the same physical chunks are reused.
    // The UniqueId is mixed into every sector checksum, so stale sectors from the old instance
    // will fail checksum verification and be silently discarded.
    Y_UNIT_TEST(PersistentBufferRestartWithStaleRecords) {
        TTestContext ctx;

        // ── Phase 1: write a record with the first DDisk instance ──────────────────
        // CreateDDisk already calls BootstrapDDisk internally.
        const TDiskHandle disk1 = ctx.CreateDDisk(13, 1);
        NDDisk::TQueryCredentials creds1 = Connect(ctx, disk1.PBServiceId, 55, 1);

        const ui64 lsn = 42;
        const TString payload = MakeData('Z', BlockSize);
        const NDDisk::TBlockSelector selector{7, 0, BlockSize};

        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds1, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk1.PBServiceId, write.release());

        // Intercept the raw write to PDisk and capture the chunk data
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk1);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);

        const ui32 writtenChunkIdx = pbWriteRaw->Get()->ChunkIdx;
        const ui32 writtenOffset   = pbWriteRaw->Get()->Offset;

        // Build a full-chunk buffer: zeroes everywhere except the written region
        TString chunkBuf(TTestContext::ChunkSize, '\0');
        {
            TString writtenData = pbWriteRaw->Get()->Data.ConvertToString();
            UNIT_ASSERT(writtenOffset + writtenData.size() <= TTestContext::ChunkSize);
            memcpy(chunkBuf.Detach() + writtenOffset, writtenData.data(), writtenData.size());
        }

        ctx.SendPDiskResponse(disk1, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        // After Phase 1, disk1's PB actor has a scheduled wakeup that will send TEvCheckSpace
        // to disk1's PDisk edge every 5000ms.  Install a filter that auto-responds to those
        // requests so the edge actor doesn't panic when the wakeup fires during Phase 2/3.
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType &&
                    ev->GetRecipientRewrite() == disk1.PDiskEdge) {
                // Auto-respond with OK so the PB actor's UpdateFreeSpaceInfo loop keeps working.
                ctx.Runtime.Send(new IEventHandle(ev->Sender, disk1.PDiskEdge,
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                    0, ev->Cookie), NodeId);
                return false; // drop the original event (don't deliver to edge)
            }
            return true; // pass through all other events
        };

        // Collect the chunk IDs owned by the first PB instance.
        // BootstrapDDisk allocates PersistentBufferInitChunks chunks starting at disk1.FirstChunkId.
        std::vector<ui32> pbChunkIds;
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            pbChunkIds.push_back(disk1.FirstChunkId + i);
        }

        // Use a known oldUniqueId.  The actual UniqueId used by disk1 is randomly generated
        // inside CreatePersistentBuffer, but we don't need to know it: we just need to pass
        // a *different* UniqueId to the second instance so that checksum verification fails.
        // We use 0 as a placeholder; the second instance will use (0 + 1) = 1.
        // Since the real UniqueId of disk1 is random and almost certainly != 1, the checksums
        // will fail and the stale records will be discarded.
        const ui64 differentUniqueId = 0;

        // ── Phase 2: restart with a NEW DDisk instance (different UniqueId) ────────
        std::unordered_map<ui32, TString> staleChunkData;
        staleChunkData[writtenChunkIdx] = chunkBuf;

        const TDiskHandle disk2 = CreateDDiskWithRestoredChunkData(ctx, 14, 1, pbChunkIds, differentUniqueId, staleChunkData);

        // ── Phase 3: verify the stale record is NOT visible ───────────────────────
        NDDisk::TQueryCredentials creds2 = Connect(ctx, disk2.PBServiceId, 55, 1);

        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds2));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 0,
            "Stale records from a previous PersistentBuffer instance must not be restored "
            "because the UniqueId mixed into checksums has changed");

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadPersistentBufferResult>(
            ctx, disk2.PBServiceId, new NDDisk::TEvReadPersistentBuffer(creds2, selector, lsn, 1, {true}));
        AssertStatus(readResult, TReplyStatus::MISSING_RECORD);
    }

    Y_UNIT_TEST(DeleteTabletChunks_RejectedWhenSyncInFlight) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(30, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 210, 1);

        const ui32 srcPDiskId = 89;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(MakeSyncSourceId(srcPDiskId, srcSlotId), 42,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        // Hold the source read: no target chunk allocation or pending chunk event exists yet,
        // but the sync may later allocate and write the target chunk.
        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(sourceRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));

        auto deleteResult = SendToDDiskAndWait<NDDisk::TEvDeleteTabletChunksResult>(
            ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        AssertStatus(deleteResult, TReplyStatus::BUSY);
    }

    Y_UNIT_TEST(DeleteTabletChunks_RejectedWhenLogInFlight) {
        // Verify that DeleteTabletChunks returns BUSY while a data chunk allocation for the
        // tablet is in flight (DataChunkAllocationsInFlight is non-empty).
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(21, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 200, 1);

        // First write to VChunk 0: DDisk sends TEvLog(snapshot) and starts formatting I/O
        // immediately. Leave formatting unreplied so the combined increment is never issued
        // and the allocation stays in flight.
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto logSnapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT(logSnapshot->Get()->CommitRecord.CommitChunks.empty());
        Y_UNUSED(logSnapshot);

        // DeleteTabletChunks must be rejected because the allocation is still in-flight.
        auto deleteResult = SendToDDiskAndWait<NDDisk::TEvDeleteTabletChunksResult>(
            ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        AssertStatus(deleteResult, TReplyStatus::BUSY);
    }

    Y_UNIT_TEST(DeleteTabletChunks_RejectedWhenAllocationQueued) {
        // Verify that DeleteTabletChunks returns BUSY when a write is queued in
        // PendingEventsForChunk (ChunkReserve exhausted, allocation not yet in log).
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(22, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 201, 1);

        const ui32 chunkBase = disk.FirstChunkId + PersistentBufferInitChunks;

        // --- Write 1 (VChunk 0): handle all PDisk requests except the refill ---
        // Consumes TWO reserve chunks: the data chunk (chunkBase) and the first integrity
        // chunk (chunkBase + 1). The integrity metadata writes are auto-served.
        {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds,
                NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
            w->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A', BlockSize)));
            SendToDDisk(ctx, disk.ServiceId, w.release());

            auto traffic = ctx.CollectAllocationTraffic(disk, true, 1, /*holdReserve=*/ true);
            UNIT_ASSERT(traffic.Reserve);
            UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[0], chunkBase + 1);
            UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[1], chunkBase);
            ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            ctx.ReplyLog(disk, *traffic.Increment);
            auto wr1 = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
            AssertStatus(wr1, TReplyStatus::OK);
        }
        // State: ChunkReserve=[chunkBase+2, chunkBase+3] (2 chunks), ReserveInFlight=true

        // --- Writes 2 and 3 (VChunks 1 and 2): drain the remaining reserve chunks ---
        // (their integrity extents reuse write 1's integrity chunk, so each takes one chunk)
        for (ui32 i = 0; i < 2; ++i) {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds,
                NDDisk::TBlockSelector(1 + i, 0, BlockSize), NDDisk::TWriteInstruction(0));
            w->AddPayloadThenChecksum(MakeAlignedRope(MakeData('B' + i, BlockSize)));
            SendToDDisk(ctx, disk.ServiceId, w.release());

            auto traffic = ctx.CollectAllocationTraffic(disk, false, 1, /*holdReserve=*/ true);
            UNIT_ASSERT(!traffic.Reserve);
            UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[0], chunkBase + 2 + i);
            ctx.SendPDiskResponse(disk, *traffic.DataWrites[0], new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            ctx.ReplyLog(disk, *traffic.Increment);
            auto wr = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
            AssertStatus(wr, TReplyStatus::OK);
        }
        // State: ChunkReserve=[] (empty), ReserveInFlight=true

        // --- Write 4 (VChunk 3): ChunkReserve empty → allocation queued, no log in-flight ---
        {
            auto w = std::make_unique<NDDisk::TEvWrite>(creds,
                NDDisk::TBlockSelector(3, 0, BlockSize), NDDisk::TWriteInstruction(0));
            w->AddPayloadThenChecksum(MakeAlignedRope(MakeData('D', BlockSize)));
            SendToDDisk(ctx, disk.ServiceId, w.release());
            // Write is now in PendingEventsForChunk[201][3]; ChunkMapIncrementsInFlight is empty.
        }

        // DeleteTabletChunks must be rejected because write 4 is pending allocation.
        auto deleteResult = SendToDDiskAndWait<NDDisk::TEvDeleteTabletChunksResult>(
            ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        AssertStatus(deleteResult, TReplyStatus::BUSY);
    }

    Y_UNIT_TEST(DeleteTabletChunks_CommittedChunkFreed) {
        // A committed chunk must not be deleted while its client data I/O is still in flight.
        // Once the I/O drains, verify the existing two-phase data/integrity deletion.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(23, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 202, 1);

        const ui32 chunkA = disk.FirstChunkId + PersistentBufferInitChunks;

        auto replyLog = [&](const auto& req) {
            auto r = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            r->Results.emplace_back(req->Get()->Lsn, req->Get()->Cookie);
            ctx.SendPDiskResponse(disk, *req, r.release());
        };

        // Write to VChunk 0: bring the chunk to committed state but hold the actual data write.
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('Z', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto traffic = ctx.CollectAllocationTraffic(disk, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[0], chunkA + 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[1], chunkA);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->ChunkIdx, chunkA);
        ctx.ReplyLog(disk, *traffic.Increment);
        // Leave the data write unreplied — the chunk has no user data yet.

        auto assertDeletionBusy = [&] {
            // Capture either the expected immediate BUSY result or the buggy deletion log. A
            // bounded simulation fails promptly instead of waiting for an unacknowledged log.
            std::unique_ptr<IEventHandle> rawDeleteResult;
            std::unique_ptr<IEventHandle> unexpectedDeleteLog;
            ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                if (!rawDeleteResult
                        && ev->GetTypeRewrite() == NDDisk::TEvDeleteTabletChunksResult::EventType) {
                    rawDeleteResult = std::move(ev);
                    return false;
                }
                if (!unexpectedDeleteLog
                        && ev->GetTypeRewrite() == NPDisk::TEvLog::EventType
                        && ev->GetRecipientRewrite() == disk.PDiskEdge) {
                    unexpectedDeleteLog = std::move(ev);
                    return false;
                }
                return true;
            };
            SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
            ui32 eventsProcessed = 0;
            ctx.Runtime.Sim([&] {
                return !rawDeleteResult && !unexpectedDeleteLog && ++eventsProcessed <= 200;
            });
            ctx.Runtime.FilterFunction = {};
            UNIT_ASSERT_C(!unexpectedDeleteLog,
                "deletion emitted a chunk-map log while client data I/O was in flight");
            UNIT_ASSERT_C(rawDeleteResult, "DeleteTabletChunks did not return BUSY");
            auto busyResult = std::unique_ptr<TEventHandle<NDDisk::TEvDeleteTabletChunksResult>>(
                reinterpret_cast<TEventHandle<NDDisk::TEvDeleteTabletChunksResult>*>(
                    rawDeleteResult.release()));
            AssertStatus(busyResult, TReplyStatus::BUSY);

            // The BUSY path must not have queued a deletion log after sending its client result.
            const TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
            ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
            for (;;) {
                auto ev = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge, sentinelEdge});
                if (ev->GetTypeRewrite() == NPDisk::TEvCheckSpace::EventType) {
                    ctx.ConsumeUnsolicitedPDiskEvent(ev);
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
                    "deletion queued PDisk traffic after returning BUSY");
                break;
            }
        };

        // The raw fallback write is still outstanding.
        assertDeletionBusy();

        // Completing the raw write is not enough: deletion must remain blocked until the DDisk
        // actor processes the completion message in its own mailbox.
        std::unique_ptr<IEventHandle> heldCompletion;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (!heldCompletion && ev->GetTypeRewrite()
                    == NDDisk::TDDiskActor::TEvPrivate::TEvDDiskIoResult::EventType) {
                heldCompletion = std::move(ev);
                return false;
            }
            return true;
        };
        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return !heldCompletion && ++eventsProcessed <= 200;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_C(heldCompletion, "DDisk I/O completion callback was not captured");
        assertDeletionBusy();

        ctx.Runtime.Send(std::move(heldCompletion), NodeId);
        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        // Reads hold the same physical chunk alive until their completion reaches the actor.
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        auto readRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(readRaw->Get()->ChunkIdx, chunkA);
        assertDeletionBusy();

        const TString payload = MakeData('Z', BlockSize);
        ctx.SendPDiskResponse(disk, *readRaw,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));

        // Phase 1 durably removes the tablet mapping and deallocates only the data chunk. The
        // integrity chunk must remain owned while the deletion record is unacknowledged.
        auto deleteDataLog = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        const auto& dataCr = deleteDataLog->Get()->CommitRecord;
        UNIT_ASSERT(dataCr.IsStartingPoint);
        UNIT_ASSERT(dataCr.CommitChunks.empty());
        UNIT_ASSERT_VALUES_EQUAL(dataCr.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(dataCr.DeleteChunks[0], chunkA);
        {
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
            UNIT_ASSERT(record.ParseFromArray(
                deleteDataLog->Get()->Data.data(), deleteDataLog->Get()->Data.size()));
            UNIT_ASSERT(record.HasSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot().TabletRecordsSize(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot().IntegrityChunksSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot().GetIntegrityChunks(0).GetChunkIdx(), chunkA + 1);
        }

        // The old extent is still quarantined: a new write from the same tablet is BUSY until the
        // first snapshot is acknowledged.
        auto blockedWrite = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(1, 0, BlockSize), NDDisk::TWriteInstruction(0));
        blockedWrite->AddPayloadThenChecksum(MakeAlignedRope(MakeData('Q', BlockSize)));
        auto blockedResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(
            ctx, disk.ServiceId, blockedWrite.release());
        AssertStatus(blockedResult, TReplyStatus::BUSY);

        replyLog(deleteDataLog);

        // Phase 2 releases the now-empty integrity chunk only after phase 1 is durable.
        auto deleteIntegrityLog = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        const auto& integrityCr = deleteIntegrityLog->Get()->CommitRecord;
        UNIT_ASSERT(integrityCr.IsStartingPoint);
        UNIT_ASSERT(integrityCr.CommitChunks.empty());
        UNIT_ASSERT_VALUES_EQUAL(integrityCr.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(integrityCr.DeleteChunks[0], chunkA + 1);
        {
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
            UNIT_ASSERT(record.ParseFromArray(
                deleteIntegrityLog->Get()->Data.data(), deleteIntegrityLog->Get()->Data.size()));
            UNIT_ASSERT(record.HasSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot().IntegrityChunksSize(), 0u);
        }

        replyLog(deleteIntegrityLog);
        auto deleteResult = WaitFromDDisk<NDDisk::TEvDeleteTabletChunksResult>(ctx);
        AssertStatus(deleteResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(IntegrityFormattingAndDataWriteRunBeforeCombinedIncrement) {
        // Reserved chunks may be formatted immediately. Header replicas and the extent format
        // run in parallel with the data write; a single combined increment is logged only after
        // the extent is Ready, and the client reply waits for that record to commit.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(24, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 203, 1);

        const ui32 dataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = dataChunk + 1;

        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, write.release());

        auto logSnap = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT(TTestContext::ParseChunkMapLog(*logSnap->Get()).HasSnapshot());
        ctx.ReplyLog(disk, *logSnap);

        std::vector<std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>> formatWrites;
        std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>> dataWrite;
        std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>> refill;
        for (ui32 guard = 0; formatWrites.size() < 4 || !dataWrite; ++guard) {
            UNIT_ASSERT_C(guard < 32, "did not observe parallel formatting I/O and the data write");
            std::unique_ptr<IEventHandle> raw = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
            const ui32 type = raw->GetTypeRewrite();
            UNIT_ASSERT_C(type != NPDisk::TEvLog::EventType,
                "combined increment must not be issued before formatting writes complete");
            if (type == NPDisk::TEvCheckSpace::EventType) {
                auto checkSpace = std::unique_ptr<TEventHandle<NPDisk::TEvCheckSpace>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvCheckSpace>*>(raw.release()));
                ctx.SendPDiskResponse(disk, *checkSpace,
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0));
                continue;
            }
            if (type == NPDisk::TEvChunkReserve::EventType) {
                UNIT_ASSERT(!refill);
                refill = std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkReserve>*>(raw.release()));
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(type, NPDisk::TEvChunkWriteRaw::EventType);
            auto writeRaw = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
                reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(raw.release()));
            if (TTestContext::IsIntegrityMetadataWrite(*writeRaw->Get())) {
                UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->ChunkIdx, integrityChunk);
                formatWrites.push_back(std::move(writeRaw));
            } else {
                UNIT_ASSERT(!dataWrite);
                UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->ChunkIdx, dataChunk);
                dataWrite = std::move(writeRaw);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(formatWrites.size(), 4u);
        UNIT_ASSERT(dataWrite);

        // Exercise the completion order most likely on a real disk: the single large extent write
        // may settle independently of the three small headers. Complete it first; the manager UT
        // asserts explicitly that the final header is what publishes readiness in this ordering.
        const auto extentIt = std::find_if(formatWrites.begin(), formatWrites.end(), [](const auto& formatWrite) {
            return formatWrite->Get()->Data.size() != sizeof(NDDisk::TIntegrityChunkHeader);
        });
        UNIT_ASSERT(extentIt != formatWrites.end());
        ctx.SendPDiskResponse(disk, **extentIt, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        for (auto& formatWrite : formatWrites) {
            if (formatWrite.get() != extentIt->get()) {
                UNIT_ASSERT_VALUES_EQUAL(formatWrite->Get()->Data.size(), sizeof(NDDisk::TIntegrityChunkHeader));
                ctx.SendPDiskResponse(disk, *formatWrite,
                    new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            }
        }

        std::unique_ptr<TEventHandle<NPDisk::TEvLog>> logIncr;
        while (!logIncr) {
            std::unique_ptr<IEventHandle> raw = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
            const ui32 type = raw->GetTypeRewrite();
            if (type == NPDisk::TEvCheckSpace::EventType) {
                auto checkSpace = std::unique_ptr<TEventHandle<NPDisk::TEvCheckSpace>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvCheckSpace>*>(raw.release()));
                ctx.SendPDiskResponse(disk, *checkSpace,
                    new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0));
                continue;
            }
            if (type == NPDisk::TEvChunkReserve::EventType) {
                UNIT_ASSERT(!refill);
                refill = std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkReserve>*>(raw.release()));
                continue;
            }
            if (type == NPDisk::TEvChunkWriteRaw::EventType) {
                auto integrityWrite = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(raw.release()));
                UNIT_ASSERT(TTestContext::IsIntegrityMetadataWrite(*integrityWrite->Get()));
                UNIT_ASSERT_VALUES_EQUAL(integrityWrite->Get()->ChunkIdx, integrityChunk);
                UNIT_ASSERT_VALUES_EQUAL(integrityWrite->Get()->Data.size(), NDDisk::IntegrityUnitSize);
                ctx.SendPDiskResponse(disk, *integrityWrite,
                    new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(type, NPDisk::TEvLog::EventType);
            logIncr = std::unique_ptr<TEventHandle<NPDisk::TEvLog>>(
                reinterpret_cast<TEventHandle<NPDisk::TEvLog>*>(raw.release()));
        }

        if (refill) {
            auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
            for (ui32 i = 0; i < refill->Get()->SizeChunks; ++i) {
                refillReply->ChunkIds.push_back(dataChunk + 2 + i);
            }
            ctx.SendPDiskResponse(disk, *refill, refillReply.release());
        }
        UNIT_ASSERT_VALUES_EQUAL(logIncr->Get()->CommitRecord.CommitChunks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(logIncr->Get()->CommitRecord.CommitChunks[0], integrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(logIncr->Get()->CommitRecord.CommitChunks[1], dataChunk);
        {
            const auto record = TTestContext::ParseChunkMapLog(*logIncr->Get());
            UNIT_ASSERT(record.HasIncrement());
            const auto& increment = record.GetIncrement();
            UNIT_ASSERT(increment.HasIntegrityChunk());
            UNIT_ASSERT_VALUES_EQUAL(increment.GetIntegrityChunk().GetChunkIdx(), integrityChunk);
            UNIT_ASSERT_VALUES_EQUAL(increment.GetIntegrityChunk().GetGeneration(), 2u);
            const auto& data = increment.GetDataChunk();
            UNIT_ASSERT_VALUES_EQUAL(data.GetTabletId(), 203u);
            UNIT_ASSERT_VALUES_EQUAL(data.GetVChunkIndex(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(data.GetChunkIdx(), dataChunk);
            UNIT_ASSERT_VALUES_EQUAL(data.GetExtentRef().GetIntegrityChunkIdx(), integrityChunk);
            UNIT_ASSERT_VALUES_EQUAL(data.GetExtentRef().GetExtentSlot(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(data.GetExtentRef().GetVChunkGeneration(), 1u);
        }

        ctx.SendPDiskResponse(disk, *dataWrite, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        auto ev = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge, sentinelEdge});
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
            "TEvWriteResult must wait for the combined increment to commit");

        ctx.ReplyLog(disk, *logIncr);
        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(SecondExtentInSameIntegrityChunkOmitsIntegrityRecord) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(44, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 220, 1);
        const ui32 firstDataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = firstDataChunk + 1;

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)).release());
        auto first = ctx.CollectAllocationTraffic(disk, true, 1);

        // Issue the second allocation before the first increment is acknowledged. The first
        // increment already establishes log ordering and owns the integrity-chunk commit.
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 1, 0, MakeData('B', BlockSize)).release());
        auto second = ctx.CollectAllocationTraffic(disk, false, 1);

        const auto firstRecord = TTestContext::ParseChunkMapLog(*first.Increment->Get());
        const auto secondRecord = TTestContext::ParseChunkMapLog(*second.Increment->Get());
        UNIT_ASSERT(firstRecord.GetIncrement().HasIntegrityChunk());
        UNIT_ASSERT_VALUES_EQUAL(
            firstRecord.GetIncrement().GetIntegrityChunk().GetChunkIdx(), integrityChunk);
        UNIT_ASSERT(!secondRecord.GetIncrement().HasIntegrityChunk());
        UNIT_ASSERT_VALUES_EQUAL(
            secondRecord.GetIncrement().GetDataChunk().GetExtentRef().GetIntegrityChunkIdx(),
            integrityChunk);
        UNIT_ASSERT(first.Increment->Get()->Lsn < second.Increment->Get()->Lsn);

        UNIT_ASSERT_VALUES_EQUAL(first.Increment->Get()->CommitRecord.CommitChunks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(first.Increment->Get()->CommitRecord.CommitChunks[0], integrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(first.Increment->Get()->CommitRecord.CommitChunks[1], firstDataChunk);
        UNIT_ASSERT_VALUES_EQUAL(second.Increment->Get()->CommitRecord.CommitChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            second.Increment->Get()->CommitRecord.CommitChunks[0],
            second.DataWrites[0]->Get()->ChunkIdx);

        ctx.SendPDiskResponse(disk, *first.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *second.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *first.Increment);
        ctx.ReplyLog(disk, *second.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(ConcurrentWritesToAllocatingChunkAllParkedUntilCommit) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(45, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 221, 1);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 7, 0, MakeData('A', BlockSize)).release(), 101);
        auto first = ctx.CollectAllocationTraffic(disk, true, 1);

        // The extent is ready and the write path is open, but allocation durability is pending.
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 7, BlockSize, MakeData('B', BlockSize)).release(), 102);

        ctx.SendPDiskResponse(disk, *first.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        // The second write is released only after the first data+integrity pair is complete.
        auto secondWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(!TTestContext::IsIntegrityMetadataWrite(*secondWrite->Get()));
        UNIT_ASSERT_VALUES_EQUAL(
            secondWrite->Get()->ChunkIdx, first.DataWrites[0]->Get()->ChunkIdx);
        ctx.SendPDiskResponse(disk, *secondWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        const TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        for (;;) {
            auto raw = ctx.Runtime.WaitForEdgeActorEvent(ctx.ClientWaitEdges({sentinelEdge}));
            if (raw->Recipient == sentinelEdge) {
                break;
            }
            if (ctx.TryAutoServeIntegrityTraffic<TEvents::TEvWakeup>(*raw)) {
                continue;
            }
            UNIT_FAIL("all write replies must remain parked until the allocation increment commits");
        }

        ctx.ReplyLog(disk, *first.Increment);
        std::set<ui64> cookies;
        while (cookies.size() < 2) {
            auto raw = ctx.Runtime.WaitForEdgeActorEvent(ctx.ClientWaitEdges());
            if (ctx.TryAutoServeIntegrityTraffic<NDDisk::TEvWriteResult>(*raw)) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(raw->GetTypeRewrite(), NDDisk::TEvWriteResult::EventType);
            const auto result = std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>>(
                reinterpret_cast<TEventHandle<NDDisk::TEvWriteResult>*>(raw.release()));
            AssertStatus(result, TReplyStatus::OK);
            cookies.insert(result->Cookie);
        }
        const std::set<ui64> expectedCookies{101, 102};
        UNIT_ASSERT(cookies == expectedCookies);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {7, 0, 2 * BlockSize}, {true}));
        auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, first.DataWrites[0]->Get()->ChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->Size, 2 * BlockSize);
        const TString expectedData = MakeData('A', BlockSize) + MakeData('B', BlockSize);
        ctx.SendPDiskResponse(disk, *dataRead,
            new NPDisk::TEvChunkReadRawResult(TRope(expectedData)));

        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), expectedData);
        const auto expectedChecksums = NDDisk::CalculatePayloadChecksums(MakeAlignedRope(expectedData));
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), expectedChecksums.size());
        for (ui32 i = 0; i < expectedChecksums.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(i), expectedChecksums[i]);
        }
    }

    Y_UNIT_TEST(ReadParkedBehindAllocatingWriteSeesData) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(46, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 222, 1);
        const TString payload = MakeData('R', BlockSize);

        SendToDDisk(ctx, disk.ServiceId, MakeWrite(creds, 3, 0, payload).release());
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {3, 0, BlockSize}, {true}));

        std::unique_ptr<TEventHandle<NPDisk::TEvLog>> increment;
        std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>> dataWrite;
        std::unique_ptr<TEventHandle<NPDisk::TEvChunkReadRaw>> dataRead;
        bool sawSnapshot = false;
        for (ui32 guard = 0; !increment || !dataWrite || !dataRead; ++guard) {
            UNIT_ASSERT_C(guard < 100, "allocation traffic did not drain the parked read");
            auto raw = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge});
            const ui32 type = raw->GetTypeRewrite();
            if (type == NPDisk::TEvChunkWriteRaw::EventType) {
                auto write = std::unique_ptr<TEventHandle<NPDisk::TEvChunkWriteRaw>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkWriteRaw>*>(raw.release()));
                if (TTestContext::IsIntegrityMetadataWrite(*write->Get())) {
                    ctx.SendPDiskResponse(disk, *write,
                        new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
                } else {
                    UNIT_ASSERT(!dataWrite);
                    dataWrite = std::move(write);
                }
            } else if (type == NPDisk::TEvChunkReadRaw::EventType) {
                UNIT_ASSERT(!dataRead);
                dataRead = std::unique_ptr<TEventHandle<NPDisk::TEvChunkReadRaw>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkReadRaw>*>(raw.release()));
            } else if (type == NPDisk::TEvLog::EventType) {
                auto log = std::unique_ptr<TEventHandle<NPDisk::TEvLog>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvLog>*>(raw.release()));
                const auto record = TTestContext::ParseChunkMapLog(*log->Get());
                if (record.HasSnapshot()) {
                    UNIT_ASSERT(!sawSnapshot);
                    sawSnapshot = true;
                    ctx.ReplyLog(disk, *log);
                } else {
                    UNIT_ASSERT(!increment);
                    increment = std::move(log);
                }
            } else if (type == NPDisk::TEvChunkReserve::EventType) {
                auto reserve = std::unique_ptr<TEventHandle<NPDisk::TEvChunkReserve>>(
                    reinterpret_cast<TEventHandle<NPDisk::TEvChunkReserve>*>(raw.release()));
                auto reply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
                for (ui32 i = 0; i < reserve->Get()->SizeChunks; ++i) {
                    reply->ChunkIds.push_back(910000 + i);
                }
                ctx.SendPDiskResponse(disk, *reserve, reply.release());
            } else if (type == NPDisk::TEvCheckSpace::EventType) {
                ctx.ConsumeUnsolicitedPDiskEvent(raw);
            } else {
                UNIT_ASSERT_C(false, "unexpected PDisk event type " << type);
            }
        }

        UNIT_ASSERT(sawSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, dataWrite->Get()->ChunkIdx);
        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *dataRead,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));

        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
        AssertNoClientReplyBeforeSentinel(
            ctx, "the allocating write must still wait for the increment");
        ctx.ReplyLog(disk, *increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(ExcessIntegrityAllocationReturnsChunkToReserve) {
        // A 140 KiB chunk has exactly one integrity extent. This makes the cancellation path
        // practical to exercise without allocating hundreds of 128 MiB data chunks.
        constexpr ui32 SmallChunkSize = 140 * 1024;
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(47, 1);
        ctx.BootstrapDDisk(disk, SmallChunkSize, 3);
        auto heldRefill = std::move(ctx.HeldBootstrapRefill);
        UNIT_ASSERT(heldRefill);

        NDDisk::TQueryCredentials firstCreds = Connect(ctx, disk.ServiceId, 223, 1);
        NDDisk::TQueryCredentials secondCreds = Connect(ctx, disk.ServiceId, 224, 1);
        const ui32 firstDataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = firstDataChunk + 1;

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(firstCreds, 0, 0, MakeData('A', BlockSize)).release());
        auto first = ctx.CollectAllocationTraffic(disk, true, 1);
        ctx.SendPDiskResponse(disk, *first.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *first.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);

        // The last reserve chunk becomes the second tablet's data chunk. Its extent cannot be
        // assigned until the first tablet is deleted, so an integrity allocation stays queued.
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(secondCreds, 0, 0, MakeData('B', BlockSize)).release());

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(firstCreds));
        auto deleteLog = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(deleteLog->Get()->CommitRecord.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(deleteLog->Get()->CommitRecord.DeleteChunks[0], firstDataChunk);
        std::unique_ptr<IEventHandle> heldDeleteResult;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!heldDeleteResult
                    && ev->GetTypeRewrite() == NDDisk::TEvDeleteTabletChunksResult::EventType) {
                heldDeleteResult = std::move(ev);
                return false;
            }
            return true;
        };
        ctx.ReplyLog(disk, *deleteLog);

        // The freed slot is assigned to the waiting second extent; formatting and the parked data
        // write then run, followed by the second allocation increment.
        auto second = ctx.CollectAllocationTraffic(disk, false, 1);
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT(heldDeleteResult);
        UNIT_ASSERT_VALUES_EQUAL(second.DataWrites[0]->Get()->ChunkIdx, firstDataChunk + 2);
        UNIT_ASSERT_VALUES_EQUAL(
            TTestContext::ParseChunkMapLog(*second.Increment->Get())
                .GetIncrement().GetDataChunk().GetExtentRef().GetIntegrityChunkIdx(),
            integrityChunk);

        const ui32 excessChunk = 777001;
        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(excessChunk);
        ctx.SendPDiskResponse(disk, *heldRefill, refillReply.release());
        auto nextRefill = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);

        ctx.SendPDiskResponse(disk, *second.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *second.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
        auto deleteResult = std::unique_ptr<TEventHandle<NDDisk::TEvDeleteTabletChunksResult>>(
            reinterpret_cast<TEventHandle<NDDisk::TEvDeleteTabletChunksResult>*>(
                heldDeleteResult.release()));
        AssertStatus(deleteResult, TReplyStatus::OK);

        // The excess chunk was returned unformatted and is reused as the next data chunk. A new
        // integrity chunk is still needed because the one-slot chunk is occupied by tablet 224.
        NDDisk::TQueryCredentials thirdCreds = Connect(ctx, disk.ServiceId, 225, 1);
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(thirdCreds, 0, 0, MakeData('C', BlockSize)).release());
        auto nextRefillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        nextRefillReply->ChunkIds.push_back(777002);
        ctx.SendPDiskResponse(disk, *nextRefill, nextRefillReply.release());
        auto third = ctx.CollectAllocationTraffic(disk, false, 1);
        UNIT_ASSERT_VALUES_EQUAL(third.DataWrites[0]->Get()->ChunkIdx, excessChunk);
    }

    Y_UNIT_TEST(DataAndIntegrityDemandDrainReserveOneToOne) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(48, 1);
        ctx.BootstrapDDisk(disk, TTestContext::ChunkSize, 1);
        auto heldRefill = std::move(ctx.HeldBootstrapRefill);
        UNIT_ASSERT(heldRefill);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 226, 1);
        const ui32 dataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = 778001;

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('D', BlockSize)).release());
        auto snapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT(TTestContext::ParseChunkMapLog(*snapshot->Get()).HasSnapshot());
        ctx.ReplyLog(disk, *snapshot);
        AssertNoClientReplyBeforeSentinel(
            ctx, "one reserve chunk can satisfy data demand but not integrity demand");

        auto refillReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        refillReply->ChunkIds.push_back(integrityChunk);
        ctx.SendPDiskResponse(disk, *heldRefill, refillReply.release());
        auto traffic = ctx.CollectAllocationTraffic(disk, false, 1);
        UNIT_ASSERT_VALUES_EQUAL(traffic.DataWrites[0]->Get()->ChunkIdx, dataChunk);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[0], integrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(traffic.Increment->Get()->CommitRecord.CommitChunks[1], dataChunk);

        ctx.SendPDiskResponse(disk, *traffic.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *traffic.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(ExtentSlotExhaustionAllocatesSecondIntegrityChunk) {
        constexpr ui32 SmallChunkSize = 140 * 1024; // one extent per integrity chunk
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(61, 1);
        ctx.BootstrapDDisk(disk, SmallChunkSize);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 240, 1);
        const ui32 firstDataChunk = disk.FirstChunkId + PersistentBufferInitChunks;

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)).release());
        auto first = ctx.CollectAllocationTraffic(disk, true, 1);
        const auto firstRecord = TTestContext::ParseChunkMapLog(*first.Increment->Get());
        UNIT_ASSERT(firstRecord.GetIncrement().HasIntegrityChunk());
        const ui32 firstIntegrityChunk =
            firstRecord.GetIncrement().GetIntegrityChunk().GetChunkIdx();
        ctx.SendPDiskResponse(disk, *first.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *first.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 1, 0, MakeData('B', BlockSize)).release());
        auto second = ctx.CollectAllocationTraffic(disk, false, 1);
        const auto secondRecord = TTestContext::ParseChunkMapLog(*second.Increment->Get());
        UNIT_ASSERT(secondRecord.GetIncrement().HasIntegrityChunk());
        const ui32 secondIntegrityChunk =
            secondRecord.GetIncrement().GetIntegrityChunk().GetChunkIdx();
        UNIT_ASSERT_VALUES_UNEQUAL(firstIntegrityChunk, secondIntegrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(second.Increment->Get()->CommitRecord.CommitChunks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(
            second.Increment->Get()->CommitRecord.CommitChunks[0], secondIntegrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(
            second.Increment->Get()->CommitRecord.CommitChunks[1], firstDataChunk + 2);
        ctx.SendPDiskResponse(disk, *second.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *second.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(CutLogDuringAllocationIncludesInFlightKeyOnce) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(49, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 227, 1);
        const TString payload = MakeData('K', BlockSize);

        SendToDDisk(ctx, disk.ServiceId, MakeWrite(creds, 5, 0, payload).release());
        auto allocation = ctx.CollectAllocationTraffic(disk, true, 1);
        const auto incrementRecord = TTestContext::ParseChunkMapLog(*allocation.Increment->Get());
        const auto& incrementData = incrementRecord.GetIncrement().GetDataChunk();

        // The increment is issued but not yet acknowledged. A later starting point must include
        // the key exactly once because PDisk replays starting points after all lower LSN records.
        ctx.Runtime.Send(new IEventHandle(disk.ServiceId, disk.PDiskEdge,
            new NPDisk::TEvCutLog(0, 0, Max<ui64>(), 0, 0, 0, 0)), NodeId);
        auto cutSnapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(
            cutSnapshot->Get()->Signature.GetUnmasked(),
            static_cast<ui32>(TLogSignature::SignatureDDiskChunkMap));
        auto pbSnapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(
            pbSnapshot->Get()->Signature.GetUnmasked(),
            static_cast<ui32>(TLogSignature::SignaturePersistentBufferChunkMap));
        const auto snapshotRecord = TTestContext::ParseChunkMapLog(*cutSnapshot->Get());
        UNIT_ASSERT(snapshotRecord.HasSnapshot());

        ui32 keyCount = 0;
        for (const auto& tablet : snapshotRecord.GetSnapshot().GetTabletRecords()) {
            if (tablet.GetTabletId() != 227) {
                continue;
            }
            for (const auto& chunk : tablet.GetChunkRefs()) {
                if (chunk.GetVChunkIndex() == 5) {
                    ++keyCount;
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetChunkIdx(), incrementData.GetChunkIdx());
                    UNIT_ASSERT_VALUES_EQUAL(
                        chunk.GetExtentRef().GetIntegrityChunkIdx(),
                        incrementData.GetExtentRef().GetIntegrityChunkIdx());
                    UNIT_ASSERT_VALUES_EQUAL(
                        chunk.GetExtentRef().GetExtentSlot(),
                        incrementData.GetExtentRef().GetExtentSlot());
                    UNIT_ASSERT_VALUES_EQUAL(
                        chunk.GetExtentRef().GetVChunkGeneration(),
                        incrementData.GetExtentRef().GetVChunkGeneration());
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keyCount, 1u);

        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *allocation.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
        ctx.ReplyLog(disk, *cutSnapshot);

        // CutLog also rewrites the PB starting point; drain it before using another fake PDisk.
        ctx.ReplyLog(disk, *pbSnapshot);

        // Boot another actor from the captured starting point and verify that the recovered key
        // routes a read to the original physical data chunk.
        const TDiskHandle recovered = ctx.RegisterDDisk(50, 1);
        ctx.BootstrapDDisk(
            recovered, TTestContext::ChunkSize, MinChunksReserved,
            &snapshotRecord, cutSnapshot->Get()->Lsn);
        NDDisk::TQueryCredentials recoveredCreds =
            Connect(ctx, recovered.ServiceId, 227, 1);
        SendToDDisk(ctx, recovered.ServiceId,
            new NDDisk::TEvRead(recoveredCreds, {5, 0, BlockSize}, {true}));
        auto integrityRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(recovered);
        UNIT_ASSERT_VALUES_EQUAL(
            integrityRead->Get()->ChunkIdx, incrementData.GetExtentRef().GetIntegrityChunkIdx());
        ctx.SendPDiskResponse(recovered, *integrityRead, new NPDisk::TEvChunkReadRawResult(
            MakeRestoredIntegrityPair(recovered.SlotId, 0x100000 + recovered.PDiskId, 227, 5,
                incrementData.GetExtentRef().GetVChunkGeneration(),
                incrementData.GetExtentRef().GetIntegrityChunkIdx(),
                incrementData.GetExtentRef().GetExtentSlot(),
                incrementRecord.GetIncrement().GetIntegrityChunk().GetGeneration(), payload)));
        auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(recovered);
        UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, incrementData.GetChunkIdx());
        ctx.SendPDiskResponse(recovered, *dataRead,
            new NPDisk::TEvChunkReadRawResult(TRope(payload)));
        auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(readResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->GetPayload(0).ConvertToString(), payload);
    }

    Y_UNIT_TEST(BootSkipsIncrementOlderThanSnapshot) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
        constexpr ui64 SnapshotLsn = 20;
        TChunkMapLogRecord snapshotRecord;
        {
            auto* snapshot = snapshotRecord.MutableSnapshot();
            auto* tablet = snapshot->AddTabletRecords();
            tablet->SetTabletId(228);
            auto* data = tablet->AddChunkRefs();
            data->SetVChunkIndex(0);
            data->SetChunkIdx(500);
            data->MutableExtentRef()->SetIntegrityChunkIdx(600);
            data->MutableExtentRef()->SetExtentSlot(0);
            data->MutableExtentRef()->SetVChunkGeneration(1);
            auto* integrity = snapshot->AddIntegrityChunks();
            integrity->SetChunkIdx(600);
            integrity->SetGeneration(1);
            snapshot->SetGenerationCounter(2);
        }

        // This conflicting increment predates the starting point and must be ignored completely.
        TChunkMapLogRecord olderIncrement;
        {
            auto* increment = olderIncrement.MutableIncrement();
            auto* integrity = increment->MutableIntegrityChunk();
            integrity->SetChunkIdx(601);
            integrity->SetGeneration(99);
            auto* data = increment->MutableDataChunk();
            data->SetTabletId(228);
            data->SetVChunkIndex(0);
            data->SetChunkIdx(501);
            data->MutableExtentRef()->SetIntegrityChunkIdx(601);
            data->MutableExtentRef()->SetExtentSlot(0);
            data->MutableExtentRef()->SetVChunkGeneration(99);
        }

        // This increment follows the snapshot and must be applied.
        TChunkMapLogRecord newerIncrement;
        {
            auto* data = newerIncrement.MutableIncrement()->MutableDataChunk();
            data->SetTabletId(228);
            data->SetVChunkIndex(1);
            data->SetChunkIdx(502);
            data->MutableExtentRef()->SetIntegrityChunkIdx(600);
            data->MutableExtentRef()->SetExtentSlot(1);
            data->MutableExtentRef()->SetVChunkGeneration(2);
        }

        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(51, 1);
        ctx.BootstrapDDisk(
            disk, TTestContext::ChunkSize, MinChunksReserved,
            &snapshotRecord, SnapshotLsn,
            {{olderIncrement, SnapshotLsn - 1}, {newerIncrement, SnapshotLsn + 1}});
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 228, 1);

        for (const auto& [vChunkIndex, expectedChunk, extentSlot, vChunkGeneration] :
                std::vector<std::tuple<ui64, ui32, ui32, ui64>>{{0, 500, 0, 1}, {1, 502, 1, 2}}) {
            const TString payload = MakeData('L', BlockSize);
            SendToDDisk(ctx, disk.ServiceId,
                new NDDisk::TEvRead(creds, {vChunkIndex, 0, BlockSize}, {true}));
            auto integrityRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(integrityRead->Get()->ChunkIdx, 600);
            ctx.SendPDiskResponse(disk, *integrityRead, new NPDisk::TEvChunkReadRawResult(
                MakeRestoredIntegrityPair(disk.SlotId, 0x100000 + disk.PDiskId,
                    228, vChunkIndex, vChunkGeneration,
                    600, extentSlot, 1, payload)));
            auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, expectedChunk);
            ctx.SendPDiskResponse(disk, *dataRead,
                new NPDisk::TEvChunkReadRawResult(TRope(payload)));
            AssertStatus(WaitFromDDisk<NDDisk::TEvReadResult>(ctx), TReplyStatus::OK);
        }
    }

    Y_UNIT_TEST(SyncAndReadRejectedWhileDeletionInFlight) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(52, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 229, 1);
        const ui32 dataChunk = disk.FirstChunkId + PersistentBufferInitChunks;

        auto initial = DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(creds, 0, 0, MakeData('A', BlockSize)),
            dataChunk, 0, MakeData('A', BlockSize), true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        auto phaseOne = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);

        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(readResult, TReplyStatus::BUSY);
        // The deletion guard runs before normal sync validation, so even an otherwise-invalid
        // empty sync is rejected specifically as BUSY.
        auto syncResult = SendToDDiskAndWait<NDDisk::TEvSyncResult>(
            ctx, disk.ServiceId, new NDDisk::TEvSync(creds));
        AssertStatus(syncResult, TReplyStatus::BUSY);

        ctx.ReplyLog(disk, *phaseOne);
        auto phaseTwo = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        ctx.ReplyLog(disk, *phaseTwo);
        AssertStatus(
            WaitFromDDisk<NDDisk::TEvDeleteTabletChunksResult>(ctx), TReplyStatus::OK);

        // A later sync can allocate the vchunk again and receives a fresh extent generation.
        constexpr ui32 SourcePDiskId = 88;
        const TActorId sourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, SourcePDiskId, 1), sourceEdge);
        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 1,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());
        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({sourceEdge});
        const TString sourcePayload = MakeData('S', BlockSize);
        ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, sourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(sourcePayload),
                MakeBlockChecksums(sourcePayload)),
            0, sourceRead->Cookie), NodeId);
        auto allocation = ctx.CollectAllocationTraffic(disk, false, 1);
        const auto allocationRecord =
            TTestContext::ParseChunkMapLog(*allocation.Increment->Get());
        UNIT_ASSERT(
            allocationRecord.GetIncrement().GetDataChunk()
                .GetExtentRef().GetVChunkGeneration() > 1);
        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *allocation.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvSyncResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(RebootBetweenDeletionPhasesReleasesIntegrityChunk) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(53, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 230, 1);
        const ui32 dataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = dataChunk + 1;

        auto initial = DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(creds, 0, 0, MakeData('A', BlockSize)),
            dataChunk, 0, MakeData('A', BlockSize), true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        auto phaseOne = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        const auto phaseOneRecord = TTestContext::ParseChunkMapLog(*phaseOne->Get());
        const ui64 phaseOneLsn = phaseOne->Get()->Lsn;
        UNIT_ASSERT_VALUES_EQUAL(
            phaseOneRecord.GetSnapshot().IntegrityChunksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            phaseOneRecord.GetSnapshot().TabletRecordsSize(), 0);
        ctx.ReplyLog(disk, *phaseOne);

        // Simulate a crash after phase 1 became durable but before phase 2 did.
        auto abandonedPhaseTwo = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(
            abandonedPhaseTwo->Get()->CommitRecord.DeleteChunks[0], integrityChunk);

        const TDiskHandle recovered = ctx.RegisterDDisk(54, 1);
        TVector<TChunkIdx> reclaimed;
        ctx.BootstrapDDisk(
            recovered, TTestContext::ChunkSize, MinChunksReserved,
            &phaseOneRecord, phaseOneLsn, {}, &reclaimed);
        UNIT_ASSERT_VALUES_EQUAL(reclaimed.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(reclaimed[0], integrityChunk);

        AssertNoClientReplyBeforeSentinel(
            ctx, "a client reply from the abandoned pre-crash deletion must not be recreated");
    }

    Y_UNIT_TEST(SharedIntegrityChunkSurvivesTabletDeletion) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(55, 1);
        NDDisk::TQueryCredentials firstCreds = Connect(ctx, disk.ServiceId, 231, 1);
        NDDisk::TQueryCredentials secondCreds = Connect(ctx, disk.ServiceId, 232, 1);
        const ui32 firstDataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = firstDataChunk + 1;

        AssertStatus(DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(firstCreds, 0, 0, MakeData('A', BlockSize)),
            firstDataChunk, 0, MakeData('A', BlockSize), true, true).WriteResult,
            TReplyStatus::OK);
        AssertStatus(DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(secondCreds, 0, 0, MakeData('B', BlockSize)),
            firstDataChunk + 2, 0, MakeData('B', BlockSize), true, false).WriteResult,
            TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(firstCreds));
        auto deleteLog = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        ctx.ReplyLog(disk, *deleteLog);
        AssertStatus(
            WaitFromDDisk<NDDisk::TEvDeleteTabletChunksResult>(ctx), TReplyStatus::OK);
        AssertNoClientReplyBeforeSentinel(
            ctx, "a shared integrity chunk must not produce a phase-2 release record");

        NDDisk::TQueryCredentials thirdCreds = Connect(ctx, disk.ServiceId, 233, 1);
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(thirdCreds, 0, 0, MakeData('C', BlockSize)).release());
        auto third = ctx.CollectAllocationTraffic(disk, false, 1);
        const auto thirdRecord = TTestContext::ParseChunkMapLog(*third.Increment->Get());
        UNIT_ASSERT(!thirdRecord.GetIncrement().HasIntegrityChunk());
        UNIT_ASSERT_VALUES_EQUAL(
            thirdRecord.GetIncrement().GetDataChunk().GetExtentRef().GetIntegrityChunkIdx(),
            integrityChunk);
        UNIT_ASSERT_VALUES_EQUAL(
            thirdRecord.GetIncrement().GetDataChunk().GetExtentRef().GetExtentSlot(), 0u);
        ctx.SendPDiskResponse(disk, *third.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.ReplyLog(disk, *third.Increment);
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);
    }

    Y_UNIT_TEST(ConcurrentDeletionsOfTabletsSharingChunk) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(56, 1);
        NDDisk::TQueryCredentials firstCreds = Connect(ctx, disk.ServiceId, 234, 1);
        NDDisk::TQueryCredentials secondCreds = Connect(ctx, disk.ServiceId, 235, 1);
        const ui32 firstDataChunk = disk.FirstChunkId + PersistentBufferInitChunks;
        const ui32 integrityChunk = firstDataChunk + 1;

        AssertStatus(DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(firstCreds, 0, 0, MakeData('A', BlockSize)),
            firstDataChunk, 0, MakeData('A', BlockSize), true, true).WriteResult,
            TReplyStatus::OK);
        AssertStatus(DoWriteWithChunkAllocation(
            ctx, disk, MakeWrite(secondCreds, 0, 0, MakeData('B', BlockSize)),
            firstDataChunk + 2, 0, MakeData('B', BlockSize), true, false).WriteResult,
            TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvDeleteTabletChunks(firstCreds), 301);
        auto firstDelete = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvDeleteTabletChunks(secondCreds), 302);
        auto secondDelete = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);

        ctx.ReplyLog(disk, *firstDelete);
        auto firstResult = WaitFromDDisk<NDDisk::TEvDeleteTabletChunksResult>(ctx);
        AssertStatus(firstResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(firstResult->Cookie, 301u);

        ctx.ReplyLog(disk, *secondDelete);
        auto release = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(release->Get()->CommitRecord.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            release->Get()->CommitRecord.DeleteChunks[0], integrityChunk);
        ctx.ReplyLog(disk, *release);

        auto secondResult = WaitFromDDisk<NDDisk::TEvDeleteTabletChunksResult>(ctx);
        AssertStatus(secondResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(secondResult->Cookie, 302u);
        AssertNoClientReplyBeforeSentinel(
            ctx, "the shared integrity chunk must be released exactly once");
    }

    Y_UNIT_TEST(BrokenAfterIncrementIssuedSkipsCallbackAndFailsParkedRepliesOnce) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(57, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 236, 1);
        constexpr ui32 SourcePDiskId = 87;
        const TActorId sourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, SourcePDiskId, 1), sourceEdge);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)).release(), 401);
        auto allocation = ctx.CollectAllocationTraffic(disk, true, 1);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 1,
            NDDisk::TBlockSelector(0, BlockSize, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release(), 402);
        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({sourceEdge});
        const TString sourcePayload = MakeData('B', BlockSize);
        ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, sourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(sourcePayload),
                MakeBlockChecksums(sourcePayload)),
            0, sourceRead->Cookie), NodeId);

        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto syncWrite = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *syncWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertNoClientReplyBeforeSentinel(
            ctx, "write and sync results must both be parked on the increment");

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvIntegrityIoResult(
                999901, TReplyStatus::ERROR, "injected failure after increment issue"));

        std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>> writeResult;
        std::unique_ptr<TEventHandle<NDDisk::TEvSyncResult>> syncResult;
        while (!writeResult || !syncResult) {
            auto raw = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge});
            if (raw->GetTypeRewrite() == NDDisk::TEvWriteResult::EventType) {
                writeResult = std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>>(
                    reinterpret_cast<TEventHandle<NDDisk::TEvWriteResult>*>(raw.release()));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    raw->GetTypeRewrite(), NDDisk::TEvSyncResult::EventType);
                syncResult = std::unique_ptr<TEventHandle<NDDisk::TEvSyncResult>>(
                    reinterpret_cast<TEventHandle<NDDisk::TEvSyncResult>*>(raw.release()));
            }
        }
        AssertStatus(writeResult, TReplyStatus::ERROR);
        AssertStatus(syncResult, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(writeResult->Cookie, 401u);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Cookie, 402u);

        // A late successful commit must not run CompleteDataChunkAllocation against state that
        // EnterBroken already drained, and must not emit replacement OK replies.
        ctx.ReplyLog(disk, *allocation.Increment);
        AssertNoClientReplyBeforeSentinel(
            ctx, "the successful late log callback must be skipped while Broken");
    }

    Y_UNIT_TEST(BrokenFailsParkedPendingEventsExactlyOnce) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(58, 1);
        ctx.BootstrapDDisk(disk, TTestContext::ChunkSize, 1);
        UNIT_ASSERT(ctx.HeldBootstrapRefill);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 237, 1);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)).release(), 501);
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}), 502);
        // The sole reserve chunk was used for data; both requests remain in the per-chunk queue
        // because no integrity chunk has been supplied.
        auto snapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT(TTestContext::ParseChunkMapLog(*snapshot->Get()).HasSnapshot());

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvIntegrityIoResult(
                999902, TReplyStatus::ERROR, "injected failure with pending events"));

        std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>> writeResult;
        std::unique_ptr<TEventHandle<NDDisk::TEvReadResult>> readResult;
        while (!writeResult || !readResult) {
            auto raw = ctx.Runtime.WaitForEdgeActorEvent({ctx.Edge});
            if (raw->GetTypeRewrite() == NDDisk::TEvWriteResult::EventType) {
                writeResult = std::unique_ptr<TEventHandle<NDDisk::TEvWriteResult>>(
                    reinterpret_cast<TEventHandle<NDDisk::TEvWriteResult>*>(raw.release()));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    raw->GetTypeRewrite(), NDDisk::TEvReadResult::EventType);
                readResult = std::unique_ptr<TEventHandle<NDDisk::TEvReadResult>>(
                    reinterpret_cast<TEventHandle<NDDisk::TEvReadResult>*>(raw.release()));
            }
        }
        AssertStatus(writeResult, TReplyStatus::ERROR);
        AssertStatus(readResult, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(writeResult->Cookie, 501u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Cookie, 502u);
        AssertNoClientReplyBeforeSentinel(
            ctx, "each pending request must be failed exactly once");

        SendToDDisk(ctx, disk.PBServiceId,
            new NDDisk::TEvGetPersistentBufferInfo(false, false));
        UNIT_ASSERT(WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx));
    }

    Y_UNIT_TEST(BrokenIgnoresLateSerializedWriteResume) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(59, 1);
        ctx.BootstrapDDisk(disk, TTestContext::ChunkSize, 1);
        Connect(ctx, disk.ServiceId, 238, 1);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvIntegrityIoResult(
                999903, TReplyStatus::ERROR, "injected failure before serialized resume"));

        // EnterBroken clears SerializedWriteResumeScheduled but cannot recall a self-message
        // already in the mailbox. The resume handler must not abort on the cleared flag.
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvHandleSerializedWriteForChunk(238, 0));

        SendToDDisk(ctx, disk.PBServiceId,
            new NDDisk::TEvGetPersistentBufferInfo(false, false));
        UNIT_ASSERT(WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx));
    }

    Y_UNIT_TEST(SyncErrorReplyStillGatedOnIncrementCommit) {
        // Unlike SyncReplyWaitsForCombinedIncrement, inject a target-write completion error after
        // a successful source read. The resulting ERROR reply obeys the same durability barrier.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(59, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 238, 1);
        constexpr ui32 SourcePDiskId = 86;
        const TActorId sourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, SourcePDiskId, 1), sourceEdge);

        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(SourcePDiskId, 1), 1,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());
        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({sourceEdge});
        const TString sourcePayload = MakeData('S', BlockSize);
        ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, sourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(sourcePayload),
                MakeBlockChecksums(sourcePayload)),
            0, sourceRead->Cookie), NodeId);
        auto allocation = ctx.CollectAllocationTraffic(disk, true, 1);

        bool changedCompletion = false;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite()
                    == NDDisk::TDDiskActor::TEvPrivate::TEvInternalSyncWriteResult::EventType) {
                auto* result = ev->CastAsLocal<
                    NDDisk::TDDiskActor::TEvPrivate::TEvInternalSyncWriteResult>();
                result->Status = TReplyStatus::ERROR;
                result->ErrorMessage = "injected target write error";
                changedCompletion = true;
            }
            return true;
        };
        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return !changedCompletion && ++eventsProcessed <= 200;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT(changedCompletion);

        AssertNoClientReplyBeforeSentinel(
            ctx, "an ERROR sync result must wait for the allocation increment");
        ctx.ReplyLog(disk, *allocation.Increment);
        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::ERROR));
    }

    Y_UNIT_TEST(IncrementLogFailureTerminatesQuietly) {
        TTestContext ctx;
        const TActorId wardenEdge =
            ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), wardenEdge);
        const TDiskHandle disk = ctx.CreateDDisk(60, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 239, 1);

        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, 0, MakeData('A', BlockSize)).release());
        auto allocation = ctx.CollectAllocationTraffic(disk, true, 1);
        ctx.SendPDiskResponse(disk, *allocation.DataWrites[0],
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertNoClientReplyBeforeSentinel(
            ctx, "the client write must still be parked before the log failure");

        auto error = std::make_unique<NPDisk::TEvLogResult>(
            NKikimrProto::INVALID_ROUND, 0, "injected owner-round loss", 0);
        error->Results.emplace_back(
            allocation.Increment->Get()->Lsn, allocation.Increment->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *allocation.Increment, error.release());

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertNoClientReplyBeforeSentinel(
            ctx, "Terminate must silently drop both the parked write and later requests");

        SendToDDisk(ctx, disk.ServiceId, new TEvents::TEvPoison());
        auto gone = ctx.Runtime.WaitForEdgeActorEvent({wardenEdge});
        UNIT_ASSERT_VALUES_EQUAL(gone->GetTypeRewrite(), TEvents::TEvGone::EventType);
    }

    Y_UNIT_TEST(IntegrityFormattingFailureEntersLiveBrokenState) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(26, 1);
        const TActorId ddiskActorId =
            ctx.Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(disk.ServiceId);
        UNIT_ASSERT(ddiskActorId);

        // A Broken transition caused by integrity formatting must not notify NodeWarden or kill
        // either the DDisk actor or its separate PersistentBuffer actor.
        const TActorId wardenEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), wardenEdge);
        std::atomic_bool sawGone = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TEvGone::EventType) {
                sawGone.store(true);
                return false;
            }
            return true;
        };

        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 205, 1);
        const ui32 srcPDiskId = 98;
        const ui32 srcSlotId = 1;
        const TActorId fakeSourceEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.RegisterService(
            MakeBlobStorageDDiskId(NodeId, srcPDiskId, srcSlotId), fakeSourceEdge);

        // The triggering request is a Sync whose source read succeeds, then waits for the target
        // data chunk and its first integrity extent to be formatted.
        auto sync = std::make_unique<NDDisk::TEvSync>(creds);
        sync->AddSegmentFromDDisk(
            MakeSyncSourceId(srcPDiskId, srcSlotId), 42,
            NDDisk::TBlockSelector(0, 0, BlockSize));
        SendToDDisk(ctx, disk.ServiceId, sync.release());

        auto sourceRead = ctx.Runtime.WaitForEdgeActorEvent({fakeSourceEdge});
        UNIT_ASSERT_VALUES_EQUAL(sourceRead->GetTypeRewrite(), static_cast<ui32>(NDDisk::TEv::EvRead));
        const TString sourcePayload = MakeData('S', BlockSize);
        ctx.Runtime.Send(new IEventHandle(sourceRead->Sender, fakeSourceEdge,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK, std::nullopt, TRope(sourcePayload),
                MakeBlockChecksums(sourcePayload)),
            0, sourceRead->Cookie), NodeId);

        auto snapshotLog = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        auto headerWrite = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(TTestContext::IsIntegrityMetadataWrite(*headerWrite->Get()));
        ctx.SendPDiskResponse(disk, *headerWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::ERROR, "injected integrity failure"));

        auto syncResult = WaitFromDDisk<NDDisk::TEvSyncResult>(ctx);
        AssertStatus(syncResult, TReplyStatus::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->Get()->Record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(syncResult->Get()->Record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::ERROR));

        // Every future DDisk data operation fails immediately with the latched reason.
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        AssertStatus(readResult, TReplyStatus::ERROR);

        auto write = std::make_unique<NDDisk::TEvWrite>(
            creds, NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('W', BlockSize)));
        auto writeResult = SendToDDiskAndWait<NDDisk::TEvWriteResult>(
            ctx, disk.ServiceId, write.release());
        AssertStatus(writeResult, TReplyStatus::ERROR);

        auto futureSync = SendToDDiskAndWait<NDDisk::TEvSyncResult>(
            ctx, disk.ServiceId, new NDDisk::TEvSync(creds));
        AssertStatus(futureSync, TReplyStatus::ERROR);
        auto deleteResult = SendToDDiskAndWait<NDDisk::TEvDeleteTabletChunksResult>(
            ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        AssertStatus(deleteResult, TReplyStatus::ERROR);

        // Duplicate raw-I/O completion, an outstanding log completion, and an arbitrary late
        // integrity completion are consumed without resurrecting allocation or sending a second
        // client reply.
        ctx.SendPDiskResponse(disk, *headerWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto snapshotReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        snapshotReply->Results.emplace_back(snapshotLog->Get()->Lsn, snapshotLog->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *snapshotLog, snapshotReply.release());
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvIntegrityIoResult(
                999999, TReplyStatus::OK));

        // Connection bookkeeping and PersistentBuffer remain operational.
        NDDisk::TQueryCredentials anotherCreds = Connect(ctx, disk.ServiceId, 206, 1);
        Y_UNUSED(anotherCreds);
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvGetPersistentBufferInfo(false, false));
        auto pbInfo = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);
        UNIT_ASSERT(pbInfo);

        UNIT_ASSERT(ctx.Runtime.WrapInActorContext(ddiskActorId, [](IActor*) {}));
        UNIT_ASSERT(!sawGone.load());
        ctx.Runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(DDiskIoCompletionIsSerializedThroughBrokenState) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(27, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 207, 1);

        // Establish a ready data chunk so the next write consists only of client data I/O.
        const TString initialPayload = MakeData('A', BlockSize);
        auto initialWrite = std::make_unique<NDDisk::TEvWrite>(
            creds, NDDisk::TBlockSelector(7, 0, BlockSize), NDDisk::TWriteInstruction(0));
        initialWrite->AddPayloadThenChecksum(MakeAlignedRope(initialPayload));
        auto initial = DoWriteWithChunkAllocation(
            ctx, disk, std::move(initialWrite),
            disk.FirstChunkId + PersistentBufferInitChunks, 0, initialPayload,
            true, true);
        AssertStatus(initial.WriteResult, TReplyStatus::OK);

        // Hold the completion-thread callback before it reaches the DDisk actor. No client reply
        // is emitted directly from the completion thread.
        std::unique_ptr<IEventHandle> heldCompletion;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (!heldCompletion && ev->GetTypeRewrite()
                    == NDDisk::TDDiskActor::TEvPrivate::TEvDDiskIoResult::EventType) {
                heldCompletion = std::move(ev);
                return false;
            }
            return true;
        };

        auto pendingWrite = std::make_unique<NDDisk::TEvWrite>(
            creds, NDDisk::TBlockSelector(7, 0, BlockSize), NDDisk::TWriteInstruction(0));
        pendingWrite->AddPayloadThenChecksum(MakeAlignedRope(MakeData('B', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, pendingWrite.release());
        auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *writeRaw,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return !heldCompletion && ++eventsProcessed <= 200;
        });
        UNIT_ASSERT_C(heldCompletion, "DDisk I/O completion callback was not captured");
        ctx.Runtime.FilterFunction = {};

        // Actor-mailbox ordering is the health barrier: once this failure is handled, the delayed
        // successful data callback must be converted into an ERROR response.
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TDDiskActor::TEvPrivate::TEvIntegrityIoResult(
                999998, TReplyStatus::ERROR, "injected integrity failure"));
        auto brokenRead = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {7, 0, BlockSize}, {true}));
        AssertStatus(brokenRead, TReplyStatus::ERROR);

        ctx.Runtime.Send(std::move(heldCompletion), NodeId);
        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::ERROR);
    }

    Y_UNIT_TEST(RestoredColdReadsSharePairLoadAndWritePreservesMetadata) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
        constexpr ui64 TabletId = 255;
        constexpr ui32 DataChunk = 500;
        constexpr ui32 IntegrityChunk = 600;

        TChunkMapLogRecord snapshotRecord;
        auto* snapshot = snapshotRecord.MutableSnapshot();
        auto* tablet = snapshot->AddTabletRecords();
        tablet->SetTabletId(TabletId);
        auto* data = tablet->AddChunkRefs();
        data->SetVChunkIndex(0);
        data->SetChunkIdx(DataChunk);
        data->MutableExtentRef()->SetIntegrityChunkIdx(IntegrityChunk);
        data->MutableExtentRef()->SetExtentSlot(0);
        data->MutableExtentRef()->SetVChunkGeneration(1);
        auto* integrity = snapshot->AddIntegrityChunks();
        integrity->SetChunkIdx(IntegrityChunk);
        integrity->SetGeneration(1);
        snapshot->SetGenerationCounter(1);

        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(77, 1);
        ctx.BootstrapDDisk(
            disk, TTestContext::ChunkSize, MinChunksReserved,
            &snapshotRecord, 10);
        NDDisk::TQueryCredentials creds =
            Connect(ctx, disk.ServiceId, TabletId, 1);
        const TString oldPayload = MakeData('R', BlockSize);
        const ui64 oldChecksum =
            NDDisk::CalculateRawChecksum(oldPayload.data(), oldPayload.size());

        std::vector<std::unique_ptr<IEventHandle>> heldReads;
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite()
                        == NPDisk::TEvCheckSpace::EventType) {
                ctx.Runtime.Send(new IEventHandle(
                    ev->Sender,
                    disk.PDiskEdge,
                    new NPDisk::TEvCheckSpaceResult(
                        NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                    0,
                    ev->Cookie), NodeId);
                return false;
            }
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType) {
                heldReads.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}), 701);
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}), 702);
        ui32 eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return heldReads.size() < 2 && ++eventsProcessed <= 300;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(heldReads.size(), 1,
            "concurrent cold reads of one checksum pair must share one metadata load");

        auto integrityRead =
            std::unique_ptr<TEventHandle<NPDisk::TEvChunkReadRaw>>(
                reinterpret_cast<TEventHandle<NPDisk::TEvChunkReadRaw>*>(
                    heldReads[0].release()));
        UNIT_ASSERT_VALUES_EQUAL(integrityRead->Get()->ChunkIdx, IntegrityChunk);
        ctx.SendPDiskResponse(disk, *integrityRead,
            new NPDisk::TEvChunkReadRawResult(
                MakeRestoredIntegrityPair(
                    disk.SlotId, 0x100000 + disk.PDiskId,
                    TabletId, 0, 1, IntegrityChunk, 0, 1, oldPayload)));

        for (ui32 i = 0; i < 2; ++i) {
            auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, DataChunk);
            ctx.SendPDiskResponse(disk, *dataRead,
                new NPDisk::TEvChunkReadRawResult(TRope(oldPayload)));
        }
        std::set<ui64> readCookies;
        for (ui32 i = 0; i < 2; ++i) {
            auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                readResult->Get()->Record.GetChecksums(0), oldChecksum);
            readCookies.insert(readResult->Cookie);
        }
        UNIT_ASSERT(readCookies == std::set<ui64>({701, 702}));

        // The pair is now cached: another read must go straight to the data chunk.
        heldReads.clear();
        ctx.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite()
                        == NPDisk::TEvCheckSpace::EventType) {
                ctx.Runtime.Send(new IEventHandle(
                    ev->Sender,
                    disk.PDiskEdge,
                    new NPDisk::TEvCheckSpaceResult(
                        NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0),
                    0,
                    ev->Cookie), NodeId);
                return false;
            }
            if (ev->GetRecipientRewrite() == disk.PDiskEdge
                    && ev->GetTypeRewrite() == NPDisk::TEvChunkReadRaw::EventType) {
                heldReads.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        eventsProcessed = 0;
        ctx.Runtime.Sim([&] {
            return heldReads.empty() && ++eventsProcessed <= 200;
        });
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(heldReads.size(), 1);
        auto cachedDataRead =
            std::unique_ptr<TEventHandle<NPDisk::TEvChunkReadRaw>>(
                reinterpret_cast<TEventHandle<NPDisk::TEvChunkReadRaw>*>(
                    heldReads[0].release()));
        UNIT_ASSERT_VALUES_EQUAL(cachedDataRead->Get()->ChunkIdx, DataChunk);
        ctx.SendPDiskResponse(disk, *cachedDataRead,
            new NPDisk::TEvChunkReadRawResult(TRope(oldPayload)));
        AssertStatus(WaitFromDDisk<NDDisk::TEvReadResult>(ctx), TReplyStatus::OK);

        // The first post-restart write updates block 1 but must carry block 0's restored
        // checksum into the new ping-pong image.
        const TString newPayload = MakeData('W', BlockSize);
        const ui64 newChecksum =
            NDDisk::CalculateRawChecksum(newPayload.data(), newPayload.size());
        SendToDDisk(ctx, disk.ServiceId,
            MakeWrite(creds, 0, BlockSize, newPayload).release());
        auto write1 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto write2 =
            ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvChunkWriteRaw>(disk);
        auto* dataWrite =
            write1->Get()->ChunkIdx == DataChunk ? write1.get() : write2.get();
        auto* integrityWrite =
            write1->Get()->ChunkIdx == DataChunk ? write2.get() : write1.get();
        UNIT_ASSERT_VALUES_EQUAL(integrityWrite->Get()->ChunkIdx, IntegrityChunk);
        const TString imageData = integrityWrite->Get()->Data.ConvertToString();
        UNIT_ASSERT_VALUES_EQUAL(imageData.size(), sizeof(NDDisk::TIntegrityBlock));
        NDDisk::TIntegrityBlock image;
        memcpy(&image, imageData.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.Magic, NDDisk::MagicIntegrityBlock);
        UNIT_ASSERT(image.Header.UsedBlocksBitmap[0] & 0x1);
        UNIT_ASSERT(image.Header.UsedBlocksBitmap[0] & 0x2);
        UNIT_ASSERT_VALUES_EQUAL(
            NDDisk::UnsealBlockChecksum(
                image.Checksums[0], disk.SlotId, 0x100000 + disk.PDiskId,
                TabletId, 0, 0),
            oldChecksum);
        UNIT_ASSERT_VALUES_EQUAL(
            NDDisk::UnsealBlockChecksum(
                image.Checksums[1], disk.SlotId, 0x100000 + disk.PDiskId,
                TabletId, 0, 1),
            newChecksum);

        ctx.SendPDiskResponse(disk, *integrityWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        ctx.SendPDiskResponse(disk, *dataWrite,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        AssertStatus(WaitFromDDisk<NDDisk::TEvWriteResult>(ctx), TReplyStatus::OK);

        SendToDDisk(ctx, disk.ServiceId,
            new NDDisk::TEvRead(creds, {0, 0, BlockSize}, {true}));
        auto untouchedRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(untouchedRead->Get()->ChunkIdx, DataChunk);
        ctx.SendPDiskResponse(disk, *untouchedRead,
            new NPDisk::TEvChunkReadRawResult(TRope(oldPayload)));
        auto untouchedResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
        AssertStatus(untouchedResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(
            untouchedResult->Get()->Record.GetChecksums(0), oldChecksum);
    }

    Y_UNIT_TEST(IntegrityMappingRestoredOnBootAndCutLogDeferred) {
        // The DataChunk -> IntegrityExtent mapping is persisted in the chunk-map snapshot and log
        // increments. On boot the DDisk must keep recovered integrity chunks that still have
        // extents, reclaim empty ones immediately (every restored chunk is Ready — a durable
        // increment is only logged after formatting), defer CutLog until replay has populated the
        // manager, lazily restore checksum pairs/bitmaps for reads, and keep the generation
        // watermark monotonic.
        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(25, 1);

        const NPDisk::TOwner Owner = 1;
        const NPDisk::TOwnerRound OwnerRound = 1;
        const ui64 snapshotLsn = 10;

        auto init = ctx.WaitPDiskRequest<NPDisk::TEvYardInit>(disk);
        TVector<ui32> ownedChunks;
        auto initReply = std::make_unique<NPDisk::TEvYardInitResult>(
            NKikimrProto::OK,
            0, 0, 0, // seek/read/write speed
            BlockSize, BlockSize, BlockSize,
            TTestContext::ChunkSize,
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

        // Starting point: one data chunk of tablet 204 mapped to an extent of integrity chunk 600,
        // plus a second committed integrity chunk 601.
        {
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
            auto* snapshot = chunkMap.MutableSnapshot();
            auto* tabletRecord = snapshot->AddTabletRecords();
            tabletRecord->SetTabletId(204);
            auto* chunkRef = tabletRecord->AddChunkRefs();
            chunkRef->SetVChunkIndex(0);
            chunkRef->SetChunkIdx(500);
            auto* extentRef = chunkRef->MutableExtentRef();
            extentRef->SetIntegrityChunkIdx(600);
            extentRef->SetExtentSlot(0);
            extentRef->SetVChunkGeneration(1);
            for (const ui32 chunkIdx : {600, 601}) {
                auto* integrityChunk = snapshot->AddIntegrityChunks();
                integrityChunk->SetChunkIdx(chunkIdx);
                integrityChunk->SetGeneration(1);
            }
            // Watermark above every restored generation: new allocations must draw past it.
            snapshot->SetGenerationCounter(3);

            TString data;
            UNIT_ASSERT(chunkMap.SerializeToString(&data));
            initReply->StartingPoints[TLogSignature::SignatureDDiskChunkMap] =
                NPDisk::TLogRecord(TLogSignature::SignatureDDiskChunkMap, TRcBuf(data), snapshotLsn);
        }
        ctx.SendPDiskResponse(disk, *init, initReply.release());

        // Log replay past the snapshot: one combined increment that first records integrity
        // chunk 602, then the data chunk that uses it.
        auto readLog = ctx.WaitPDiskRequest<NPDisk::TEvReadLog>(disk);
        auto readLogReply = std::make_unique<NPDisk::TEvReadLogResult>(
            NKikimrProto::OK,
            readLog->Get()->Position,
            readLog->Get()->Position,
            true, // end of log
            0,    // status flags
            "",
            Owner);
        {
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
            auto* increment = chunkMap.MutableIncrement();
            auto* chunk = increment->MutableIntegrityChunk();
            chunk->SetChunkIdx(602);
            chunk->SetGeneration(1);
            auto* dataInc = increment->MutableDataChunk();
            dataInc->SetTabletId(204);
            dataInc->SetVChunkIndex(1);
            dataInc->SetChunkIdx(501);
            auto* extentRef = dataInc->MutableExtentRef();
            extentRef->SetIntegrityChunkIdx(602);
            extentRef->SetExtentSlot(0);
            extentRef->SetVChunkGeneration(1);
            TString data;
            UNIT_ASSERT(chunkMap.SerializeToString(&data));
            readLogReply->Results.emplace_back(TLogSignature::SignatureDDiskChunkMap, TRcBuf(data),
                snapshotLsn + 1);
        }
        // YardInit already registered this actor as the CutLog recipient, so PDisk may ask for a
        // new starting point before this ReadLog result completes recovery. Send both messages
        // from the PDisk edge to preserve their order at the DDisk mailbox.
        ctx.Runtime.Send(new IEventHandle(disk.ServiceId, disk.PDiskEdge,
            new NPDisk::TEvCutLog(0, 0, Max<ui64>(), 0, 0, 0, 0)), NodeId);
        ctx.SendPDiskResponse(disk, *readLog, readLogReply.release());

        // End-of-log: chunk 600 is kept for its restored extent; empty chunk 601 is reclaimed
        // immediately; chunk 602 was restored from the increment and has an extent. No header
        // rewrites — every restored chunk is already Ready.
        auto reclaimLog = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(reclaimLog->Get()->CommitRecord.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(reclaimLog->Get()->CommitRecord.DeleteChunks[0], 601u);
        {
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
            UNIT_ASSERT(record.ParseFromArray(
                reclaimLog->Get()->Data.data(), reclaimLog->Get()->Data.size()));
            UNIT_ASSERT(record.HasSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot().IntegrityChunksSize(), 2u);
        }
        ctx.ReplyLog(disk, *reclaimLog);

        // The deferred CutLog is processed only after ApplyMappingSnapshot and the empty-chunk
        // reclaim above. Its snapshot must contain the complete recovered mapping and watermark.
        auto cutLogSnapshot = ctx.WaitPDiskRequestNoAutoServe<NPDisk::TEvLog>(disk);
        UNIT_ASSERT(cutLogSnapshot->Get()->CommitRecord.IsStartingPoint);
        UNIT_ASSERT(cutLogSnapshot->Get()->CommitRecord.CommitChunks.empty());
        UNIT_ASSERT(cutLogSnapshot->Get()->CommitRecord.DeleteChunks.empty());
        {
            const auto record = TTestContext::ParseChunkMapLog(*cutLogSnapshot->Get());
            UNIT_ASSERT(record.HasSnapshot());
            const auto& snapshot = record.GetSnapshot();
            UNIT_ASSERT_VALUES_EQUAL(snapshot.GetGenerationCounter(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.IntegrityChunksSize(), 2u);

            std::map<ui64, std::tuple<ui32, ui32, ui32, ui64>> refs;
            for (const auto& tablet : snapshot.GetTabletRecords()) {
                UNIT_ASSERT_VALUES_EQUAL(tablet.GetTabletId(), 204u);
                for (const auto& chunk : tablet.GetChunkRefs()) {
                    refs.emplace(chunk.GetVChunkIndex(), std::make_tuple(
                        chunk.GetChunkIdx(),
                        chunk.GetExtentRef().GetIntegrityChunkIdx(),
                        chunk.GetExtentRef().GetExtentSlot(),
                        chunk.GetExtentRef().GetVChunkGeneration()));
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(refs.size(), 2u);
            UNIT_ASSERT(refs.at(0) == std::make_tuple(500u, 600u, 0u, 1u));
            UNIT_ASSERT(refs.at(1) == std::make_tuple(501u, 602u, 0u, 1u));
        }
        ctx.ReplyLog(disk, *cutLogSnapshot);

        auto reserve = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
        auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        for (ui32 i = 0; i < PersistentBufferInitChunks + MinChunksReserved; ++i) {
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + i);
        }
        ctx.SendPDiskResponse(disk, *reserve, reserveReply.release());
        for (ui32 i = 0; i < PersistentBufferInitChunks; ++i) {
            auto log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
            ctx.ReplyLog(disk, *log);
        }
        auto checkSpace = ctx.WaitPDiskRequest<NPDisk::TEvCheckSpace>(disk);
        ctx.SendPDiskResponse(disk, *checkSpace,
            new NPDisk::TEvCheckSpaceResult(NKikimrProto::OK, 0, 0, 0, 0, 0, 0, 0, "", 0));

        UNIT_ASSERT(ctx.AutoServedIntegrityWriteChunks.empty());

        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 204, 1);
        for (const auto& [vChunkIndex, chunkIdx, integrityChunkIdx] :
                std::vector<std::tuple<ui64, ui32, ui32>>{{0, 500, 600}, {1, 501, 602}}) {
            const TString payload = MakeData('R', BlockSize);
            SendToDDisk(ctx, disk.ServiceId, new NDDisk::TEvRead(creds, {vChunkIndex, 0, BlockSize}, {true}));
            auto integrityRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(integrityRead->Get()->ChunkIdx, integrityChunkIdx);
            UNIT_ASSERT_VALUES_EQUAL(integrityRead->Get()->Size,
                NDDisk::IntegrityPairSlots * NDDisk::IntegrityUnitSize);
            ctx.SendPDiskResponse(disk, *integrityRead, new NPDisk::TEvChunkReadRawResult(
                MakeRestoredIntegrityPair(disk.SlotId, 0x100000 + disk.PDiskId,
                    204, vChunkIndex, 1,
                    integrityChunkIdx, 0, 1, payload)));

            auto dataRead = ctx.WaitPDiskRequest<NPDisk::TEvChunkReadRaw>(disk);
            UNIT_ASSERT_VALUES_EQUAL(dataRead->Get()->ChunkIdx, chunkIdx);
            ctx.SendPDiskResponse(disk, *dataRead,
                new NPDisk::TEvChunkReadRawResult(TRope(payload)));
            auto readResult = WaitFromDDisk<NDDisk::TEvReadResult>(ctx);
            AssertStatus(readResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.ChecksumsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Record.GetChecksums(0),
                NDDisk::CalculateRawChecksum(payload.data(), payload.size()));
        }

        // A write to a restored chunk needs no allocation, log record or integrity formatting,
        // but it does persist a new ping-pong slot alongside the data write.
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('W', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, write.release());
        auto writeRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT_VALUES_EQUAL(writeRaw->Get()->ChunkIdx, 500u);
        ctx.SendPDiskResponse(disk, *writeRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto writeResult = WaitFromDDisk<NDDisk::TEvWriteResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);

        UNIT_ASSERT(!ctx.AutoServedIntegrityWriteChunks.empty());

        // A brand-new allocation (VChunk 2) draws its generation past the persisted watermark
        // (3) and reuses a free slot of the lowest restored chunk. The extent format write and
        // the reserve refill are auto-served; the increment carries the extent ref and does not
        // re-commit chunk 600.
        auto write2 = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(2, 0, BlockSize), NDDisk::TWriteInstruction(0));
        write2->AddPayloadThenChecksum(MakeAlignedRope(MakeData('X', BlockSize)));
        SendToDDisk(ctx, disk.ServiceId, write2.release());
        auto traffic = ctx.CollectAllocationTraffic(disk, false, 1);
        {
            const auto record = TTestContext::ParseChunkMapLog(*traffic.Increment->Get());
            UNIT_ASSERT(record.HasIncrement());
            UNIT_ASSERT(!record.GetIncrement().HasIntegrityChunk());
            const auto& ref = record.GetIncrement().GetDataChunk().GetExtentRef();
            UNIT_ASSERT_VALUES_EQUAL(ref.GetVChunkGeneration(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(ref.GetIntegrityChunkIdx(), 600u);
            UNIT_ASSERT_VALUES_EQUAL(ref.GetExtentSlot(), 1u);
        }
        for (const ui32 chunkIdx : ctx.AutoServedIntegrityWriteChunks) {
            UNIT_ASSERT_VALUES_EQUAL(chunkIdx, 600u);
        }
    }

    Y_UNIT_TEST(DeleteTabletChunks_NoChunks) {
        // DeleteTabletChunks must return OK immediately (no PDisk I/O) when the
        // tablet has never allocated any chunks.
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(24, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.ServiceId, 203, 1);

        auto deleteResult = SendToDDiskAndWait<NDDisk::TEvDeleteTabletChunksResult>(
            ctx, disk.ServiceId, new NDDisk::TEvDeleteTabletChunks(creds));
        AssertStatus(deleteResult, TReplyStatus::OK);
    }

    // Helper: query FreeSectors from the PB actor.
    ui32 GetPBFreeSectors(TTestContext& ctx, const TDiskHandle& disk) {
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvGetPersistentBufferInfo(false, false));
        auto info = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);
        return info->Get()->FreeSectors;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: sectors allocated for a write must be freed when the disk write
    //         fails.
    //
    // Injection strategy: intercept TEvWritePersistentBufferPart (the internal
    // message that OnComplete sends back to the PB actor after TEvChunkWriteRaw
    // is acknowledged) and replace it with a failed version.  This avoids
    // sending TEvChunkWriteRawResult(ERROR) which would terminate the actor.
    //
    // Covers: HandleWritePart → else branch → PersistentBufferSpaceAllocator.Free(inflight.OccupiedSectors)
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferWriteFailFreesAllocatedSectors) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(30, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 70, 1);

        const ui64 lsn = 1;
        const TString payload = MakeData('A', BlockSize);
        const NDDisk::TBlockSelector selector{5, 0, BlockSize};

        // Capture free-sector count before the write attempt.
        const ui32 freeBefore = GetPBFreeSectors(ctx, disk);

        // Install a filter that intercepts TEvWritePersistentBufferPart (the
        // internal completion message) and replaces it with a failed version.
        // We only want to intercept the first non-erase write part.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted &&
                    ev->GetTypeRewrite() == NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::EventType) {
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>*>(ev.get());
                if (!orig->Get()->IsErase) {
                    intercepted = true;
                    // Replace with a failed version carrying the same cookies.
                    auto failed = std::make_unique<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>(
                        orig->Get()->InflightCookie,
                        orig->Get()->PartCookie,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                        "injected write failure");
                    ev.reset(new IEventHandle(ev->Recipient, ev->Sender, failed.release(), 0, ev->Cookie));
                }
            }
            return true;
        };

        // Send write request.
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        // Acknowledge the raw disk write with OK so the actor stays alive.
        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // The write must fail (filter replaced the completion with an error).
        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        ctx.Runtime.FilterFunction = {};
        UNIT_ASSERT_C(intercepted, "Filter must have fired");
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(writeResult->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "Write should have failed");

        // The record must NOT appear in the list.
        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 0,
            "Failed write must not leave a record in the persistent buffer");

        // Free-sector count must be restored to the value before the write.
        const ui32 freeAfter = GetPBFreeSectors(ctx, disk);
        UNIT_ASSERT_VALUES_EQUAL_C(freeAfter, freeBefore,
            "Sectors allocated for a failed write must be freed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: when one erase succeeds and another fails, the successfully erased
    //         record must be removed from the persistent buffer while the failed
    //         one stays.
    //
    // ClearPersistentBufferRecords is called only when resultStatus == true
    // (the disk write succeeded).  On failure the record remains in
    // PersistentBuffers.
    //
    // Injection strategy: intercept TEvWritePersistentBufferPart for the erase
    // of lsn=20 and replace it with a failed version.  The erase of lsn=10 is
    // allowed to succeed normally.
    //
    // Covers: HandleErasePart → ClearPersistentBufferRecords called only on
    //         success (the fix).
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferPartialEraseSuccess) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(31, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 71, 1);

        // Write two records with different LSNs.
        const TString payload = MakeData('B', BlockSize);
        const NDDisk::TBlockSelector selector{6, 0, BlockSize};

        auto doWrite = [&](ui64 lsn) {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        };

        doWrite(10);
        doWrite(20);

        // Verify both records are present.
        {
            auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
                ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
            AssertStatus(listResult, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(listResult->Get()->Record.RecordsSize(), 2);
        }

        // Erase lsn=10 successfully (no filter).
        {
            SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, 10));
            auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
            AssertStatus(eraseResult, TReplyStatus::OK);
        }

        // Install a filter that injects a failure for the erase of lsn=20.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted &&
                    ev->GetTypeRewrite() == NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::EventType) {
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>*>(ev.get());
                if (orig->Get()->IsErase) {
                    intercepted = true;
                    auto failed = std::make_unique<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>(
                        orig->Get()->InflightCookie,
                        orig->Get()->PartCookie,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                        "injected erase failure",
                        /*isErase=*/true);
                    ev.reset(new IEventHandle(ev->Recipient, ev->Sender, failed.release(), 0, ev->Cookie));
                }
            }
            return true;
        };

        // Erase lsn=20 — the filter injects a failure for the disk write completion.
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, 20));
        auto eraseRaw2 = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw2, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto eraseResult2 = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        ctx.Runtime.FilterFunction = {};

        UNIT_ASSERT_C(intercepted, "Filter must have fired for lsn=20 erase");
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(eraseResult2->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "Erase of lsn=20 should have failed");

        // lsn=10 was successfully erased → removed.
        // lsn=20 erase failed → record must remain in PersistentBuffers.
        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 1,
            "The record whose erase succeeded must be removed; "
            "the record whose erase failed must remain");
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.GetRecords(0).GetLsn(), 20u,
            "The record whose erase failed (lsn=20) must still be present");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: writing the same (tabletId, generation, lsn) record a second time
    //         after it is already committed must NOT issue a new disk write —
    //         the actor must reply OK immediately from in-memory state.
    //
    // Covers: ProcessPersistentBufferWrite → duplicate-record fast-path that
    //         calls SendReply with OK without touching the disk.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferDuplicateWriteNoRedisk) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(32, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 72, 1);

        const ui64 lsn = 5;
        const TString payload = MakeData('C', BlockSize);
        const NDDisk::TBlockSelector selector{7, 0, BlockSize};

        // First write: goes to disk.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // Second write with the same (tabletId, generation, lsn) and identical payload:
        // must return OK immediately without any PDisk I/O.
        {
            auto write2 = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write2->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write2.release());

            // The response must arrive without any intervening PDisk request.
            // Use a sentinel actor: if a PDisk request arrives before the write result,
            // the test will fail because WaitForEdgeActorEvent returns the PDisk event first.
            auto writeResult2 = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult2, TReplyStatus::OK);

            // Confirm no PDisk write was issued by checking the edge is empty.
            TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
            ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
            auto ev = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge, sentinelEdge});
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
                "Duplicate write of an already-committed record must not issue a disk write");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: when two erase requests share the same in-flight disk write and
    //         that write fails, BOTH erase replies must report failure.
    //
    // Before the fix, only the original inflight's status was updated on
    // failure; shared inflights kept their default OK status and could reply
    // with success even though the underlying write failed.
    //
    // Setup: write lsn=5, then send two separate TEvBatchErasePersistentBuffer
    // requests for lsn=5 before the PDisk write completes.  The second erase
    // shares the first's disk write (same partCookie via
    // PersistentBufferEraseInflightsByRecord).
    // Inject a failure for the single shared disk write completion.
    //
    // NOTE: We use TEvBatchErasePersistentBuffer (single-record batch) instead
    // of TEvErasePersistentBuffer because:
    //   - TEvErasePersistentBuffer routes to BarrierErasePersistentBuffer which
    //     calls MoveBarrier; a second call with the same lsn triggers an
    //     assertion ("new barrier lsn is not bigger than previous").
    //   - TEvBatchErasePersistentBuffer with a single lsn routes to
    //     ErasePersistentBuffer (PersistentBufferBarriersManager::Erase returns
    //     nullopt when lsns.size() < 2), which contains the shared-inflight
    //     path that Fix 4 corrects.
    //
    // Covers: Handle(TEvWritePersistentBufferPart) → propagate error to
    //         inflight2 before calling HandleErasePart(inflight2, ...) (the fix).
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferSharedEraseInflightFailurePropagation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(33, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 73, 1);

        const ui64 lsn = 5;
        const TString payload = MakeData('D', BlockSize);
        const NDDisk::TBlockSelector selector{8, 0, BlockSize};

        // Write lsn=5 and complete it successfully.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // Send two TEvBatchErasePersistentBuffer requests for the same lsn=5
        // before the PDisk write completes.  The second erase will share the
        // first's disk write via PersistentBufferEraseInflightsByRecord.
        //
        // A single-record batch bypasses the fast-erase path
        // (PersistentBufferBarriersManager::Erase returns nullopt when
        // lsns.size() < 2) and goes directly to ErasePersistentBuffer where
        // the shared-inflight logic lives.
        {
            auto batchErase1 = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
            batchErase1->AddErase(lsn, creds.Generation);
            SendToDDisk(ctx, disk.PBServiceId, batchErase1.release());
        }
        {
            auto batchErase2 = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
            batchErase2->AddErase(lsn, creds.Generation);
            SendToDDisk(ctx, disk.PBServiceId, batchErase2.release());
        }

        // There must be exactly one PDisk write (shared by both erases).
        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // Install a filter that injects a failure for the shared erase disk write.
        bool intercepted = false;
        ctx.Runtime.FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (!intercepted &&
                    ev->GetTypeRewrite() == NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::EventType) {
                auto* orig = reinterpret_cast<TEventHandle<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>*>(ev.get());
                if (orig->Get()->IsErase) {
                    intercepted = true;
                    auto failed = std::make_unique<NDDisk::TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart>(
                        orig->Get()->InflightCookie,
                        orig->Get()->PartCookie,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                        "injected shared erase failure",
                        /*isErase=*/true);
                    ev.reset(new IEventHandle(ev->Recipient, ev->Sender, failed.release(), 0, ev->Cookie));
                }
            }
            return true;
        };

        // Respond OK to PDisk — the filter will replace the internal completion
        // with an error before it reaches the PB actor.
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        // Collect both erase results.
        auto eraseResult1 = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        auto eraseResult2 = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        ctx.Runtime.FilterFunction = {};

        UNIT_ASSERT_C(intercepted, "Filter must have fired for the shared erase disk write");

        // Both erase replies must report failure — the fix propagates the error
        // to all shared inflights before calling HandleErasePart on them.
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(eraseResult1->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "First erase reply must report failure when the shared disk write failed");
        UNIT_ASSERT_C(
            static_cast<TReplyStatus::E>(eraseResult2->Get()->Record.GetStatus()) != TReplyStatus::OK,
            "Second erase reply must report failure when the shared disk write failed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: EraseCookie mismatch crash (VERIFY failed at line 504).
    //
    // Scenario:
    //   1. Write record lsn=10.
    //   2. Send TEvErasePersistentBuffer(lsn=10) → BarrierErasePersistentBuffer.
    //      This creates inflight_barrier with Erases[C_barrier] = [(lsn=10, gen=1)].
    //      It does NOT register in PersistentBufferEraseInflightsByRecord.
    //      Hold the PDisk write so the I/O is still in flight.
    //   3. Send TEvBatchErasePersistentBuffer(lsn=10) → ErasePersistentBuffer.
    //      This registers PersistentBufferEraseInflightsByRecord[{tabletId,gen,lsn=10}]
    //      = {EraseCookie=C_batch, OperationsCookie=[op_batch]}.
    //      It issues its own PDisk write.
    //   4. Complete the barrier PDisk write (step 2).
    //      Handle(TEvWritePersistentBufferPart) fires for inflight_barrier with
    //      partCookie=C_barrier. It iterates Erases[C_barrier] = [(lsn=10, gen=1)],
    //      finds PersistentBufferEraseInflightsByRecord[{tabletId,gen,lsn=10}] with
    //      EraseCookie=C_batch ≠ C_barrier → Y_ABORT_UNLESS fires → CRASH.
    //
    // After the fix the assertion is replaced with a safe check (skip if cookie
    // doesn't match), so the test must complete without crashing.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferEraseCookieMismatchNoCrash) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(34, 1);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 74, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('E', BlockSize);
        const NDDisk::TBlockSelector selector{9, 0, BlockSize};

        // Step 1: write lsn=10 and complete it.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw,
                new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // Step 2: send TEvErasePersistentBuffer(lsn=10) → BarrierErasePersistentBuffer.
        // Hold the PDisk write so the barrier I/O stays in flight.
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn));
        auto barrierWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // Step 3: send TEvBatchErasePersistentBuffer(lsn=10) → ErasePersistentBuffer.
        // This registers a new EraseCookie in PersistentBufferEraseInflightsByRecord
        // for the same record, and issues its own PDisk write.
        {
            auto batchErase = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
            batchErase->AddErase(lsn, creds.Generation);
            SendToDDisk(ctx, disk.PBServiceId, batchErase.release());
        }
        auto batchWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);

        // Step 4: complete the barrier PDisk write.
        // Before the fix this triggers Y_ABORT_UNLESS(it->second.EraseCookie == partCookie)
        // at ddisk_actor_persistent_buffer.cpp:504 because the EraseCookie in
        // PersistentBufferEraseInflightsByRecord was overwritten by the batch erase in step 3.
        ctx.SendPDiskResponse(disk, *barrierWriteRaw,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto barrierEraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(barrierEraseResult, TReplyStatus::OK);

        // Complete the batch erase PDisk write and collect its result.
        ctx.SendPDiskResponse(disk, *batchWriteRaw,
            new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto batchEraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(batchEraseResult, TReplyStatus::OK);

        // After both erases complete the record must be gone.
        auto listResult = SendToDDiskAndWait<NDDisk::TEvListPersistentBufferResult>(
            ctx, disk.PBServiceId, new NDDisk::TEvListPersistentBuffer(creds));
        AssertStatus(listResult, TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult->Get()->Record.RecordsSize(), 0,
            "Record must be erased after both barrier and batch erase complete");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: PreprocessPersistentBufferWrite rejects a new write with OVERFILL
    // when free sectors drop below MinFreeSectorsReserve.
    //
    // Motivation: barrier movement and fast erases write to a new sector first
    // and free the old one only after the disk write completes. A plain write
    // that exhausts the free pool would block those higher-priority operations.
    //
    // Setup: use MinFreeSectorsReserve = TotalSectors - 1 so that the very
    // first write leaves exactly (TotalSectors - 2) free sectors – one below
    // the reserve threshold.  The second write therefore hits the OVERFILL
    // guard in PreprocessPersistentBufferWrite without ever touching the disk.
    // After erasing the first record (freeing its 2 sectors) the free count
    // rises back to (TotalSectors - 1 + 1) = TotalSectors - barrier_sector,
    // which is >= MinFreeSectorsReserve, so the third write succeeds.
    //
    // Covers: PreprocessPersistentBufferWrite → MinFreeSectorsReserve check.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferLowFreeSpaceRejectWrite) {
        // 4 chunks × (128 MB / 4096) = 131072 sectors total.
        // Reserve = 131071 → after one write (2 sectors) we have 131070 free,
        // which is < 131071, so the second write is rejected immediately.
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize; // 32768
        constexpr ui32 TotalSectors = PersistentBufferInitChunks * SectorsPerChunk; // 131072
        constexpr ui32 Reserve = TotalSectors - 1; // 131071

        NDDisk::TPersistentBufferFormat fmt;
        // MaxChunks == InitChunks: disk is at capacity from the start so the
        // MinFreeSectorsReserve guard in PreprocessPersistentBufferWrite fires.
        fmt.MaxChunks = PersistentBufferInitChunks;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 4096_MB; // large enough to never hit per-tablet limit
        fmt.MinFreeSectorsReserve = Reserve;

        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(30, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 40, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('X', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        // ── Write 1: exactly fits (131072 free ≥ 131071 reserve) ─────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // ── Write 2: must be rejected immediately (131070 < 131071 reserve) ──
        // No PDisk I/O expected – the preprocess check fires before allocation.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn + 1, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OVERFILL);
        }

        // ── Erase lsn=10 via barrier: frees 2 data sectors (net +1 after
        //    barrier sector allocation) so free rises back to >= reserve. ─────
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvErasePersistentBuffer(creds, lsn));
        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);

        // ── Write 3: free sectors restored above reserve – must succeed. ──────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn + 1, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: a duplicate write request (same tabletId/generation/lsn that is
    // already committed in PersistentBuffers) must succeed even when free
    // sectors are below MinFreeSectorsReserve.
    //
    // Duplicate requests do not allocate any new disk space – they reuse the
    // already-committed record.  The preprocess function returns OK (after
    // sending a reply itself) before the free-space check is reached.
    //
    // Covers: PreprocessPersistentBufferWrite → committed-duplicate fast-path
    //         returns false (reply sent) before MinFreeSectorsReserve guard.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferDuplicateBypassesLowFreeSpaceCheck) {
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize;
        constexpr ui32 TotalSectors = PersistentBufferInitChunks * SectorsPerChunk;
        constexpr ui32 Reserve = TotalSectors - 1; // tight reserve

        NDDisk::TPersistentBufferFormat fmt;
        // MaxChunks == InitChunks: disk is at capacity from the start.
        fmt.MaxChunks = PersistentBufferInitChunks;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 4096_MB;
        fmt.MinFreeSectorsReserve = Reserve;

        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(31, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 50, 1);

        const ui64 lsn = 20;
        const TString payload = MakeData('Y', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        // ── Write 1: commit the record. ───────────────────────────────────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // Sanity: a different lsn at this point would be rejected with OVERFILL.
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn + 1, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OVERFILL);
        }

        // ── Duplicate write (same lsn): must return OK, no PDisk I/O. ─────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            // The committed-duplicate fast-path in PreprocessPersistentBufferWrite
            // replies with OK immediately without going to disk.
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: when MaxChunks > InitChunks (the persistent buffer can still grow),
    // the free-space guard in PreprocessPersistentBufferWrite must NOT fire even
    // when free sectors have dropped below MinFreeSectorsReserve.
    //
    // Rationale: the guard condition is:
    //   OwnedChunks.size() >= MaxChunks  &&  GetFreeSpace() < MinFreeSectorsReserve
    //
    // With MaxChunks > OwnedChunks.size(), the first sub-condition is false, so
    // the whole check is bypassed.  The write proceeds normally (goes to disk),
    // because the system still has room to allocate a new chunk when needed.
    //
    // This is the complementary case to PersistentBufferLowFreeSpaceRejectWrite:
    // the same tight Reserve, but MaxChunks is one larger than InitChunks so the
    // second write is NOT rejected even though free < Reserve.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferLowFreeSpaceAllowsWhenCanGrow) {
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize; // 32768
        constexpr ui32 TotalSectors = PersistentBufferInitChunks * SectorsPerChunk; // 131072
        constexpr ui32 Reserve = TotalSectors - 1; // 131071 – same tight reserve

        NDDisk::TPersistentBufferFormat fmt;
        // MaxChunks is one more than InitChunks: OwnedChunks.size() will be 4
        // after bootstrap, which is strictly less than MaxChunks (5), so the guard
        // precondition "OwnedChunks.size() >= MaxChunks" is always false here.
        fmt.MaxChunks = PersistentBufferInitChunks + 1;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 4096_MB;
        fmt.MinFreeSectorsReserve = Reserve;

        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(32, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 60, 1);

        const ui64 lsn = 10;
        const TString payload = MakeData('X', BlockSize);
        const NDDisk::TBlockSelector selector{3, 0, BlockSize};

        // ── Write 1: succeeds; leaves 131070 free < 131071 reserve ───────────
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // ── Write 2: free is now below Reserve, but MaxChunks > OwnedChunks,  ──
        // so the guard is skipped and the write reaches the disk (no OVERFILL). ──
        // Compare: with MaxChunks == InitChunks this exact write returns OVERFILL
        // (see PersistentBufferLowFreeSpaceRejectWrite).
        {
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, lsn + 1, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(payload));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            // Guard is bypassed → PDisk write is expected.
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests for proactive chunk preallocation
    // (TPersistentBufferFormat::PreallocateFreeSpaceThresholdPercent).
    //
    // PreprocessPersistentBufferWrite issues a chunk allocation in advance when
    //   freeSpace * 100 < PreallocateFreeSpaceThresholdPercent * ownedChunks * SectorInChunk
    //   && ownedChunks < MaxChunks
    // i.e. when free space drops below PreallocateFreeSpaceThresholdPercent percent of the
    // currently owned capacity, a new chunk is allocated before the buffer runs
    // out of space.
    //
    // Shared math (4 init chunks, 128 MB chunks, 4 KB sectors):
    //   SectorsPerChunk = 32768, TotalSectors = 4 x 32768 = 131072.
    //   Each 128-block write occupies 129 sectors (128 data + 1 header).
    //   With PreallocateFreeSpaceThresholdPercent = 99 the trigger threshold is
    //   free < 99 x 4 x 32768 / 100 = 129761.28:
    //     before write 11: free = 131072 - 10*129 = 129782 -> no trigger;
    //     before write 12: free = 131072 - 11*129 = 129653 -> trigger.
    // ─────────────────────────────────────────────────────────────────────────

    NDDisk::TPersistentBufferFormat MakeProactiveAllocationFormat(ui32 maxChunks, ui32 preallocateFreeSpaceThresholdPercent) {
        NDDisk::TPersistentBufferFormat fmt;
        fmt.MaxChunks = maxChunks;
        fmt.InitChunks = PersistentBufferInitChunks;
        fmt.MaxInMemoryCache = BlockSize * 128;
        fmt.MaxChunkRestoreInflight = 8;
        fmt.UpdateFreeSpaceInfoMilliseconds = 5000;
        fmt.PerTabletStorageLimit = 4096_MB; // large enough to never hit the per-tablet limit
        fmt.MinFreeSectorsReserve = 256;
        fmt.PreallocateFreeSpaceThresholdPercent = preallocateFreeSpaceThresholdPercent;
        return fmt;
    }

    // Helper: one 128-block write with a full PDisk round-trip, must succeed.
    void DoPBWriteRoundTrip(TTestContext& ctx, const TDiskHandle& disk,
            const NDDisk::TQueryCredentials& creds, ui64 lsn, char fill) {
        const ui32 writeSize = BlockSize * 128;
        const NDDisk::TBlockSelector selector{3, 0, writeSize};
        auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
            creds, selector, lsn, NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(MakeData(fill, writeSize)));
        SendToDDisk(ctx, disk.PBServiceId, write.release());

        auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
        ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
        AssertStatus(writeResult, TReplyStatus::OK);
    }

    // Helper: query AllocatedChunks from the PB actor.
    ui32 GetPBAllocatedChunks(TTestContext& ctx, const TDiskHandle& disk) {
        SendToDDisk(ctx, disk.PBServiceId, new NDDisk::TEvGetPersistentBufferInfo(false, false));
        auto info = WaitFromDDisk<NDDisk::TEvPersistentBufferInfo>(ctx);
        return info->Get()->AllocatedChunks;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: when free space drops below PreallocateFreeSpaceThresholdPercent percent, a
    // new chunk is allocated in advance, while there is still plenty of free
    // space (long before OVERFILL would fire).
    //
    // Covers: PreprocessPersistentBufferWrite -> proactive
    //         IssuePersistentBufferChunkAllocation() branch.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferProactiveChunkAllocation) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(33, 1,
            MakeProactiveAllocationFormat(256 /*maxChunks*/, 99 /*preallocateFreeSpaceThresholdPercent*/));
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 80, 1);

        // ── Writes 1..11: free space stays above the 99% threshold, so no
        //    preallocation happens; each write is a plain PDisk round-trip. ────
        for (ui32 i = 0; i < 11; ++i) {
            DoPBWriteRoundTrip(ctx, disk, creds, /*lsn=*/10 + i, 'A' + i);
        }
        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks);

        // ── Write 12: before the write free = 129653 < 129761.28, so the
        //    preprocess step proactively issues a chunk allocation.  The write
        //    itself still proceeds normally (there is plenty of space). ────────
        {
            const ui32 writeSize = BlockSize * 128;
            const NDDisk::TBlockSelector selector{3, 0, writeSize};
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, /*lsn=*/21, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(MakeData('M', writeSize)));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            // The data write goes out first (sent by the PB actor before the
            // DDisk actor processes the allocation request).
            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            UNIT_ASSERT(pbWriteRaw->Get()->Data.size() > 0);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

            // The DDisk actor takes a chunk from its bootstrap reserve and logs
            // the updated PB chunk map.
            auto log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            ctx.SendPDiskResponse(disk, *log, logReply.release());

            // Consuming a reserved chunk drops the reserve below
            // MinChunksReserved, so the DDisk actor refills it.
            auto reserve = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
            UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, 1u);
            auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + PersistentBufferInitChunks + MinChunksReserved);
            ctx.SendPDiskResponse(disk, *reserve, reserveReply.release());

            // The write itself must have completed successfully.
            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }

        // ── The PB actor must now own one extra chunk, allocated proactively
        //    while ~129k of 131k sectors were still free. ──────────────────────
        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks + 1);

        // Free space must account for 12 writes and the extra chunk:
        // 5 x 32768 - 12 x 129 = 162292.
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize;
        UNIT_ASSERT_VALUES_EQUAL(GetPBFreeSectors(ctx, disk),
            (PersistentBufferInitChunks + 1) * SectorsPerChunk - 12 * 129);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: proactive preallocation must NOT fire when the buffer already owns
    // MaxChunks chunks, even though free space is below the threshold.
    //
    // Covers: PreprocessPersistentBufferWrite -> "ownedChunks < MaxChunks"
    //         sub-condition of the proactive allocation check.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferProactiveAllocationSkippedAtMaxChunks) {
        TTestContext ctx;
        // MaxChunks == InitChunks: the buffer cannot grow.
        const TDiskHandle disk = ctx.CreateDDisk(34, 1,
            MakeProactiveAllocationFormat(PersistentBufferInitChunks, 99));
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 81, 1);

        // 12 writes: from write 12 on, free space is below the 99% threshold,
        // but ownedChunks == MaxChunks so no allocation may be issued.  Every
        // write is a plain round-trip; if the actor issued an allocation, the
        // TEvLog would arrive at the PDisk edge ahead of the next write's
        // TEvChunkWriteRaw and DoPBWriteRoundTrip would fail on the event type.
        for (ui32 i = 0; i < 12; ++i) {
            DoPBWriteRoundTrip(ctx, disk, creds, /*lsn=*/10 + i, 'A' + i);
        }

        // Still exactly InitChunks chunks; free space is below the threshold.
        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks);
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize;
        const ui32 free = GetPBFreeSectors(ctx, disk);
        UNIT_ASSERT_VALUES_EQUAL(free, PersistentBufferInitChunks * SectorsPerChunk - 12 * 129);
        UNIT_ASSERT_C(ui64(free) * 100 < ui64(99) * PersistentBufferInitChunks * SectorsPerChunk,
            "free space must be below the preallocation threshold for the test to be meaningful");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: PreallocateFreeSpaceThresholdPercent = 0 disables proactive allocation
    // completely (freeSpace * 100 < 0 is never true), even when the buffer is
    // allowed to grow (MaxChunks > InitChunks).
    //
    // Covers: PreprocessPersistentBufferWrite -> threshold sub-condition of the
    //         proactive allocation check.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferProactiveAllocationDisabledByZeroThreshold) {
        TTestContext ctx;
        const TDiskHandle disk = ctx.CreateDDisk(35, 1,
            MakeProactiveAllocationFormat(256 /*maxChunks*/, 0 /*PreallocateFreeSpaceThresholdPercent*/));
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 82, 1);

        // Same 12 writes as in PersistentBufferProactiveChunkAllocation, where
        // write 12 would have triggered preallocation with threshold 99.  With
        // threshold 0 nothing may be allocated.
        for (ui32 i = 0; i < 12; ++i) {
            DoPBWriteRoundTrip(ctx, disk, creds, /*lsn=*/10 + i, 'A' + i);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Integration tests for proactive chunk deallocation
    // (TPersistentBufferFormat::DeallocateFreeSpaceThresholdPercent /
    //  DeallocateThresholdSeconds).
    //
    // ProcessDeallocatePersistentBufferChunk (called after every Free()) locks
    // owned chunks round-robin and, once a locked chunk turns out to be fully
    // free, sends TEvPrivate::TEvDeallocatePersistentBufferChunk to the DDisk
    // actor, which writes a chunk-map log record with the physical chunk in
    // DeleteChunks, causing PDisk to release the chunk immediately as part of
    // that log commit.
    //
    // Shared setup: reuse MakeProactiveAllocationFormat / DoPBWriteRoundTrip /
    // GetPBFreeSectors from the proactive-allocation tests above. 12 writes with
    // PreallocateFreeSpaceThresholdPercent = 99 leave the buffer with 5 owned
    // chunks (4 original + 1 proactively allocated), where the 5th chunk (index
    // PersistentBufferInitChunks) is fully free -- exactly the state needed to
    // exercise deallocation.
    // ─────────────────────────────────────────────────────────────────────────

    NDDisk::TPersistentBufferFormat MakeDeallocationFormat(ui32 deallocateFreeSpaceThresholdPercent, ui32 deallocateThresholdSeconds) {
        NDDisk::TPersistentBufferFormat fmt = MakeProactiveAllocationFormat(256 /*maxChunks*/, 99 /*preallocateFreeSpaceThresholdPercent*/);
        fmt.DeallocateFreeSpaceThresholdPercent = deallocateFreeSpaceThresholdPercent;
        fmt.DeallocateThresholdSeconds = deallocateThresholdSeconds;
        return fmt;
    }

    // Drives 12 writes (as in PersistentBufferProactiveChunkAllocation) to reach
    // PersistentBufferInitChunks + 1 owned chunks, with the extra (last) chunk
    // fully free. Returns the physical chunk id of that extra chunk.
    ui32 ReachFiveChunksWithLastFullyFree(TTestContext& ctx, const TDiskHandle& disk, const NDDisk::TQueryCredentials& creds) {
        for (ui32 i = 0; i < 11; ++i) {
            DoPBWriteRoundTrip(ctx, disk, creds, /*lsn=*/10 + i, 'A' + i);
        }
        {
            const ui32 writeSize = BlockSize * 128;
            const NDDisk::TBlockSelector selector{3, 0, writeSize};
            auto write = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                creds, selector, /*lsn=*/21, NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(TRope(MakeData('M', writeSize)));
            SendToDDisk(ctx, disk.PBServiceId, write.release());

            auto pbWriteRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
            ctx.SendPDiskResponse(disk, *pbWriteRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

            auto log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
            auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
            logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
            ctx.SendPDiskResponse(disk, *log, logReply.release());

            auto reserve = ctx.WaitPDiskRequest<NPDisk::TEvChunkReserve>(disk);
            UNIT_ASSERT_VALUES_EQUAL(reserve->Get()->SizeChunks, 1u);
            auto reserveReply = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
            reserveReply->ChunkIds.push_back(disk.FirstChunkId + PersistentBufferInitChunks + MinChunksReserved);
            ctx.SendPDiskResponse(disk, *reserve, reserveReply.release());

            auto writeResult = WaitFromDDisk<NDDisk::TEvWritePersistentBufferResult>(ctx);
            AssertStatus(writeResult, TReplyStatus::OK);
        }
        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks + 1);
        return disk.FirstChunkId + PersistentBufferInitChunks; // the proactively allocated (5th) chunk
    }

    // Erase lsn=10 (the very first write, on the very first owned chunk) via
    // TEvBatchErasePersistentBuffer with fast erases effectively bypassed
    // (EnableFastErases = false in the format), so the erase goes through the
    // plain ErasePersistentBuffer -> ClearPersistentBufferRecords path, which is
    // the one that calls ProcessDeallocatePersistentBufferChunk().
    void EraseFirstRecordSlowPath(TTestContext& ctx, const TDiskHandle& disk, const NDDisk::TQueryCredentials& creds) {
        auto batchErase = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
        batchErase->AddErase(/*lsn=*/10, creds.Generation);
        SendToDDisk(ctx, disk.PBServiceId, batchErase.release());

        auto eraseRaw = ctx.WaitPDiskRequest<NPDisk::TEvChunkWriteRaw>(disk);
        ctx.SendPDiskResponse(disk, *eraseRaw, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::OK, ""));

        auto eraseResult = WaitFromDDisk<NDDisk::TEvErasePersistentBufferResult>(ctx);
        AssertStatus(eraseResult, TReplyStatus::OK);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: after 12 writes leave a 5th, fully-free chunk and DeallocateFreeSpaceThresholdPercent
    // is set very low (so the free-space precondition is always true once the buffer can shrink),
    // erasing a record frees sectors and triggers ProcessDeallocatePersistentBufferChunk, which
    // round-robins the lock through the owned chunks (starting at chunk 0) until it reaches the
    // fully-free 5th chunk, then issues TEvPrivate::TEvDeallocatePersistentBufferChunk -> a
    // persistent-buffer-chunk-map log record whose commit record's DeleteChunks contains that
    // physical chunk, causing PDisk to release it immediately.
    //
    // Covers: ProcessDeallocatePersistentBufferChunk, TPersistentBufferSpaceAllocator::LockNextChunk/
    //         DeallocateChunk, TDDiskActor::Handle(TEvDeallocatePersistentBufferChunk).
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferProactiveDeallocationAfterErase) {
        TTestContext ctx;
        auto fmt = MakeDeallocationFormat(/*deallocateFreeSpaceThresholdPercent=*/90, /*deallocateThresholdSeconds=*/1);
        fmt.EnableFastErases = false;
        const TDiskHandle disk = ctx.CreateDDisk(40, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 90, 1);

        const ui32 extraChunk = ReachFiveChunksWithLastFullyFree(ctx, disk, creds);
        const ui32 freeSectorsBeforeErase = GetPBFreeSectors(ctx, disk);

        // Erase the very first write (lsn=10, physically on chunk #0): frees 129
        // sectors and triggers ProcessDeallocatePersistentBufferChunk(). The
        // free-space precondition is satisfied (5 owned chunks, well above the
        // 90% threshold), so the allocator starts round-robin locking, beginning
        // at chunk #0. Chunks 0..3 all still hold occupied sectors from the 12
        // writes, so each lock attempt fails and reschedules a 1-second wakeup
        // with forceToNextChunk=true, advancing the lock to the next chunk. Only
        // the 5th chunk (never written to) is fully free, so the deallocation
        // succeeds on the 5th lock attempt (chunks 0,1,2,3,4).
        EraseFirstRecordSlowPath(ctx, disk, creds);

        UNIT_ASSERT_VALUES_EQUAL(GetPBFreeSectors(ctx, disk), freeSectorsBeforeErase + 129);

        // Drain the persistent-buffer-chunk-map log record for the deallocation
        // (the simulated clock advances through the intermediate 1-second wakeup
        // cycles automatically while waiting for this event). Its commit record's
        // DeleteChunks must contain exactly the extra (physically freed) chunk,
        // which causes PDisk to release it immediately as part of the commit.
        // Regression check: the physical chunk being deallocated must go into
        // DeleteChunks, not CommitChunks (CommitChunks would tell PDisk to keep
        // the chunk committed/owned rather than release it).
        auto log = ctx.WaitPDiskRequest<NPDisk::TEvLog>(disk);
        UNIT_ASSERT_VALUES_EQUAL(log->Get()->CommitRecord.DeleteChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(log->Get()->CommitRecord.DeleteChunks[0], extraChunk);
        UNIT_ASSERT_C(log->Get()->CommitRecord.CommitChunks.empty(),
            "the deallocated chunk must not appear in CommitChunks");
        auto logReply = std::make_unique<NPDisk::TEvLogResult>(NKikimrProto::OK, 0, "", 0);
        logReply->Results.emplace_back(log->Get()->Lsn, log->Get()->Cookie);
        ctx.SendPDiskResponse(disk, *log, logReply.release());

        // The deallocated chunk's capacity (32768 sectors) must be gone from the
        // free pool: free sectors after deallocation must be exactly
        // (freeSectorsBeforeErase + 129 [reclaimed by the erase] - 32768 [chunk
        // capacity removed]).
        constexpr ui32 SectorsPerChunk = TTestContext::ChunkSize / BlockSize;
        UNIT_ASSERT_VALUES_EQUAL(GetPBFreeSectors(ctx, disk),
            freeSectorsBeforeErase + 129 - SectorsPerChunk);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: DeallocateFreeSpaceThresholdPercent = 100 disables proactive
    // deallocation completely (freeSpace * 100 > ownedChunks * SectorInChunk * 100
    // is never true), even though a 5th, fully-free chunk exists and an erase
    // frees additional sectors.
    //
    // Covers: ProcessDeallocatePersistentBufferChunk -> canDeallocate threshold
    //         sub-condition.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferDeallocationDisabledByFullThreshold) {
        TTestContext ctx;
        auto fmt = MakeDeallocationFormat(/*deallocateFreeSpaceThresholdPercent=*/100, /*deallocateThresholdSeconds=*/1);
        fmt.EnableFastErases = false;
        const TDiskHandle disk = ctx.CreateDDisk(41, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 91, 1);

        ReachFiveChunksWithLastFullyFree(ctx, disk, creds);
        const ui32 freeSectorsBeforeErase = GetPBFreeSectors(ctx, disk);

        EraseFirstRecordSlowPath(ctx, disk, creds);
        UNIT_ASSERT_VALUES_EQUAL(GetPBFreeSectors(ctx, disk), freeSectorsBeforeErase + 129);

        // No deallocation may be issued: verify no TEvLog / TEvChunkForget shows
        // up at the PDisk edge by racing a sentinel wakeup through the same
        // edge actor. If a chunk-map log request were in flight it would arrive
        // before the sentinel (FIFO per-actor delivery), causing the assertion
        // below to observe the log/forget event's recipient instead of the
        // sentinel edge.
        TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        auto ev = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge, sentinelEdge});
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
            "no PDisk request (deallocation) should be issued when DeallocateFreeSpaceThresholdPercent=100");

        // Still exactly PersistentBufferInitChunks + 1 owned chunks worth of free space.
        UNIT_ASSERT_VALUES_EQUAL(GetPBFreeSectors(ctx, disk), freeSectorsBeforeErase + 129);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test: deallocation must not fire while the buffer owns exactly InitChunks
    // chunks, even if free space is at 100% (canDeallocate requires
    // ownedChunks > InitChunks).
    //
    // Covers: ProcessDeallocatePersistentBufferChunk -> "ownedChunks > InitChunks"
    //         sub-condition of canDeallocate.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferDeallocationSkippedAtInitChunks) {
        TTestContext ctx;
        auto fmt = MakeDeallocationFormat(/*deallocateFreeSpaceThresholdPercent=*/1, /*deallocateThresholdSeconds=*/1);
        fmt.EnableFastErases = false;
        fmt.PreallocateFreeSpaceThresholdPercent = 0; // keep exactly InitChunks owned chunks
        const TDiskHandle disk = ctx.CreateDDisk(42, 1, fmt);
        NDDisk::TQueryCredentials creds = Connect(ctx, disk.PBServiceId, 92, 1);

        DoPBWriteRoundTrip(ctx, disk, creds, /*lsn=*/10, 'A');
        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks);

        EraseFirstRecordSlowPath(ctx, disk, creds);

        // No deallocation may be issued: the buffer owns exactly InitChunks
        // chunks, so canDeallocate's "ownedChunks > InitChunks" sub-condition is
        // always false, regardless of how low the free-space threshold is set.
        TActorId sentinelEdge = ctx.Runtime.AllocateEdgeActor(NodeId, __FILE__, __LINE__);
        ctx.Runtime.Send(new IEventHandle(sentinelEdge, ctx.Edge, new TEvents::TEvWakeup()), NodeId);
        auto ev = ctx.Runtime.WaitForEdgeActorEvent({disk.PDiskEdge, sentinelEdge});
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Recipient, sentinelEdge,
            "no PDisk request (deallocation) should be issued when ownedChunks == InitChunks");

        UNIT_ASSERT_VALUES_EQUAL(GetPBAllocatedChunks(ctx, disk), PersistentBufferInitChunks);
    }

    Y_UNIT_TEST(ChecksumsOnToOffTransitionBreaksDDisk) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
        constexpr ui64 TabletId = 501;
        constexpr ui32 DataChunkIdx = 700;
        constexpr ui32 IntegrityChunkIdx = 701;

        TChunkMapLogRecord enabledSnapshot;
        auto* snapshot = enabledSnapshot.MutableSnapshot();
        auto* tablet = snapshot->AddTabletRecords();
        tablet->SetTabletId(TabletId);
        auto* chunk = tablet->AddChunkRefs();
        chunk->SetVChunkIndex(3);
        chunk->SetChunkIdx(DataChunkIdx);
        chunk->MutableExtentRef()->SetIntegrityChunkIdx(IntegrityChunkIdx);
        chunk->MutableExtentRef()->SetExtentSlot(4);
        chunk->MutableExtentRef()->SetVChunkGeneration(9);
        auto* integrityChunk = snapshot->AddIntegrityChunks();
        integrityChunk->SetChunkIdx(IntegrityChunkIdx);
        integrityChunk->SetGeneration(8);
        snapshot->SetGenerationCounter(9);

        TTestContext ctx;
        NDDisk::TDDiskConfig config;
        config.EnableChecksums = false;
        const TDiskHandle disk =
            ctx.RegisterDDisk(81, 1, std::nullopt, config);
        ctx.BootstrapDDisk(
            disk,
            4u << 20,
            MinChunksReserved,
            &enabledSnapshot,
            10);

        NDDisk::TQueryCredentials creds;
        creds.TabletId = TabletId;
        creds.Generation = 1;
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {3, 0, BlockSize},
                NDDisk::TReadInstruction(true)));
        AssertStatus(readResult, TReplyStatus::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(
            readResult->Get()->Record.GetErrorReason(),
            "integrity chunks while EnableChecksums=false");
    }

    Y_UNIT_TEST(ChecksumsOffToOnTransitionBreaksDDisk) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;

        TChunkMapLogRecord disabledSnapshot;
        disabledSnapshot.SetChecksumsDisabled(true);
        auto* snapshot = disabledSnapshot.MutableSnapshot();
        auto* tablet = snapshot->AddTabletRecords();
        tablet->SetTabletId(502);
        auto* chunk = tablet->AddChunkRefs();
        chunk->SetVChunkIndex(4);
        chunk->SetChunkIdx(702);

        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(82, 1);
        ctx.BootstrapDDisk(
            disk,
            4u << 20,
            MinChunksReserved,
            &disabledSnapshot,
            10);

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 502;
        creds.Generation = 1;
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {4, 0, BlockSize},
                NDDisk::TReadInstruction(true)));
        AssertStatus(readResult, TReplyStatus::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(
            readResult->Get()->Record.GetErrorReason(),
            "data chunks without integrity chunks while EnableChecksums=true");
    }

    Y_UNIT_TEST(ChecksumsEnabledRejectsDataChunkWithoutExtent) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;

        TChunkMapLogRecord transitionedSnapshot;
        transitionedSnapshot.SetChecksumsDisabled(true);
        auto* snapshot = transitionedSnapshot.MutableSnapshot();
        auto* tablet = snapshot->AddTabletRecords();
        tablet->SetTabletId(505);
        auto* chunk = tablet->AddChunkRefs();
        chunk->SetVChunkIndex(5);
        chunk->SetChunkIdx(704);
        auto* integrityChunk = snapshot->AddIntegrityChunks();
        integrityChunk->SetChunkIdx(705);
        integrityChunk->SetGeneration(1);

        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(85, 1);
        ctx.BootstrapDDisk(
            disk,
            4u << 20,
            MinChunksReserved,
            &transitionedSnapshot,
            10);

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 505;
        creds.Generation = 1;
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {5, 0, BlockSize},
                NDDisk::TReadInstruction(true)));
        AssertStatus(readResult, TReplyStatus::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(
            readResult->Get()->Record.GetErrorReason(),
            "data chunks without integrity extents while EnableChecksums=true");
    }

    Y_UNIT_TEST(EmptyDDiskAllowsChecksumsModeChange) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;

        TChunkMapLogRecord disabledSnapshot;
        disabledSnapshot.SetChecksumsDisabled(true);
        disabledSnapshot.MutableSnapshot();

        TTestContext ctx;
        const TDiskHandle disk = ctx.RegisterDDisk(83, 1);
        ctx.BootstrapDDisk(
            disk,
            4u << 20,
            MinChunksReserved,
            &disabledSnapshot,
            10);

        const NDDisk::TQueryCredentials creds =
            Connect(ctx, disk.ServiceId, 503, 1);
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {0, 0, BlockSize},
                NDDisk::TReadInstruction(true)));
        AssertStatus(readResult, TReplyStatus::OK);
    }

    Y_UNIT_TEST(IntegrityChunksWithChecksumsDisabledBreaksDDisk) {
        using TChunkMapLogRecord =
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;

        TChunkMapLogRecord enabledSnapshot;
        auto* integrityChunk =
            enabledSnapshot.MutableSnapshot()->AddIntegrityChunks();
        integrityChunk->SetChunkIdx(703);
        integrityChunk->SetGeneration(1);

        TTestContext ctx;
        NDDisk::TDDiskConfig config;
        config.EnableChecksums = false;
        const TDiskHandle disk =
            ctx.RegisterDDisk(84, 1, std::nullopt, config);
        ctx.BootstrapDDisk(
            disk,
            4u << 20,
            MinChunksReserved,
            &enabledSnapshot,
            10);

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 504;
        creds.Generation = 1;
        auto readResult = SendToDDiskAndWait<NDDisk::TEvReadResult>(
            ctx,
            disk.ServiceId,
            new NDDisk::TEvRead(
                creds,
                {0, 0, BlockSize},
                NDDisk::TReadInstruction(true)));
        AssertStatus(readResult, TReplyStatus::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(
            readResult->Get()->Record.GetErrorReason(),
            "integrity chunks while EnableChecksums=false");
    }

} // Y_UNIT_TEST_SUITE

} // NKikimr
