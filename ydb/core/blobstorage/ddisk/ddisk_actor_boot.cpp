#include "ddisk_actor.h"
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <algorithm>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    void TDDiskActor::ValidateChecksumsModeAfterLogReplay() {
        if (!Config.EnableChecksums) {
            if (!RestoredIntegrityMapping.IntegrityChunks.empty()) {
                EnterBroken(TStringBuilder()
                    << "restored " << RestoredIntegrityMapping.IntegrityChunks.size()
                    << " integrity chunks while EnableChecksums=false");
            }
            return;
        }

        absl::flat_hash_set<TIntegrityManager::TDataChunkKey> coveredDataChunks;
        coveredDataChunks.reserve(RestoredIntegrityMapping.Extents.size());
        for (const auto& extent : RestoredIntegrityMapping.Extents) {
            coveredDataChunks.insert(extent.Key);
        }

        size_t dataChunkCount = 0;
        size_t uncoveredDataChunkCount = 0;
        for (const auto& [tabletId, tablet] : Tablets) {
            const auto& chunks = tablet.ChunkRefs;
            for (const auto& [vChunkIndex, chunkRef] : chunks) {
                if (!chunkRef.ChunkIdx) {
                    continue;
                }
                ++dataChunkCount;
                if (!coveredDataChunks.contains({tabletId, vChunkIndex})) {
                    ++uncoveredDataChunkCount;
                }
            }
        }

        if (dataChunkCount && RestoredIntegrityMapping.IntegrityChunks.empty()) {
            EnterBroken(TStringBuilder()
                << "restored " << dataChunkCount
                << " data chunks without integrity chunks while EnableChecksums=true");
            return;
        }

        if (uncoveredDataChunkCount) {
            EnterBroken(TStringBuilder()
                << "restored " << uncoveredDataChunkCount << " of " << dataChunkCount
                << " data chunks without integrity extents while EnableChecksums=true");
        }
    }

    void TDDiskActor::InitPDiskInterface() {
        Y_ABORT_UNLESS(!IsPersistentBufferActor);
        YDB_LOG_DEBUG("TDDiskActor::InitPDiskInterface",
            {"marker", "BSDD01"},
            {"DDiskId", DDiskId},
            {"PDiskActorId", BaseInfo.PDiskActorID});
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvYardInit(BaseInfo.InitOwnerRound, TVDiskID(Info->GroupID,
            Info->GroupGeneration, BaseInfo.VDiskIdShort), BaseInfo.PDiskGuid, SelfId(), SelfId(), BaseInfo.VDiskSlotId,
            0 /*groupSizeInUnits*/, !Config.ForcePDiskFallback /*getUringRouterClient*/,
            Config.IdleSpinUs, Config.DevNullMode));
    }

    void TDDiskActor::Handle(NPDisk::TEvYardInitResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_INFO("TDDiskActor::Handle(TEvYardInitResult)",
            {"marker", "BSDD02"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvYardInitResult)")) {
            return;
        }
        Y_ABORT_UNLESS(msg.DiskFormat);

        PDiskParams = std::move(msg.PDiskParams);
        DiskFormat = std::move(msg.DiskFormat);
        OwnedChunksOnBoot = std::move(msg.OwnedChunks);
#if defined(__linux__)
        if (!Config.ForcePDiskFallback) {
            UringRouter = std::move(msg.UringRouter);
        }
        if ((Config.DevNullMode && !UringRouter)
                || (UringRouter && UringRouter->GetConfig().DevNullMode != Config.DevNullMode)) {
            BeginStopping("DDisk requires a shared io_uring router with matching DevNullMode");
            return;
        }
        if (!UringRouter) {
            YDB_LOG_INFO("TDDiskActor::Handle(TEvYardInitResult) "
                "UringRouter is not set, all further I/O will be routed "
                "through PDisk",
                {"marker", "BSDD17"},
                {"DDiskId", DDiskId},
                {"PDiskActorId", BaseInfo.PDiskActorID});
        }
#endif

        if (DiskFormat->ChunkSize > ExpectedPDiskChunkSize) {
            YDB_LOG_NOTICE("TDDiskActor::Handle(TEvYardInitResult) PDisk chunk is bigger than expected, "
                "the space PDisk reserved for its per-sector metadata is left unused; "
                "format the PDisk with PhysicalChunkSize to avoid it",
                {"marker", "BSDD56"},
                {"DDiskId", DDiskId},
                {"chunkSize", DiskFormat->ChunkSize},
                {"userAccessibleChunkSize", DiskFormat->GetUserAccessibleChunkSize()},
                {"expectedChunkSize", ExpectedPDiskChunkSize});
        }

        if (Config.EnableChecksums) {
            // The integrity manager needs the chunk size, so it is created here rather than in the ctor.
            // VDiskSlotId + PDiskGuid identify this DDisk in TIntegrityChunkHeader.
            IntegrityManager.emplace( DiskFormat->ChunkSize, BaseInfo.VDiskSlotId, BaseInfo.PDiskGuid,
                Config.IntegrityChecksumCacheBytes);
        }

        if (const auto it = msg.StartingPoints.find(TLogSignature::SignatureDDiskChunkMap); it != msg.StartingPoints.end()) {
            NPDisk::TLogRecord& record = it->second;
            ChunkMapSnapshotLsn = record.Lsn;
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
            const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
            Y_ABORT_UNLESS(success);
            Y_ABORT_UNLESS(chunkMap.HasSnapshot());
            const auto& snapshot = chunkMap.GetSnapshot();
            for (const auto& tabletRecord : snapshot.GetTabletRecords()) {
                if (tabletRecord.GetChunkRefs().empty()) {
                    continue;
                }
                auto& tabletChunkMap = Tablets[tabletRecord.GetTabletId()].ChunkRefs;
                for (const auto& chunkRef : tabletRecord.GetChunkRefs()) {
                    SetDataChunkMapping(tabletRecord.GetTabletId(), &tabletChunkMap[chunkRef.GetVChunkIndex()], chunkRef.GetChunkIdx());
                    ++*Counters.Chunks.ChunksOwned;
                    if (chunkRef.HasExtentRef()) {
                        const auto& ref = chunkRef.GetExtentRef();
                        RestoredIntegrityMapping.Extents.push_back({
                            .Key = {tabletRecord.GetTabletId(), chunkRef.GetVChunkIndex()},
                            .DataChunkIdx = chunkRef.GetChunkIdx(),
                            .Ref = {ref.GetIntegrityChunkIdx(), ref.GetExtentSlot(), ref.GetVChunkGeneration()},
                        });
                    }
                }
            }
            for (const auto& chunk : snapshot.GetIntegrityChunks()) {
                RestoredIntegrityMapping.IntegrityChunks.push_back(
                    {chunk.GetChunkIdx(), chunk.GetGeneration()});
                CommittedIntegrityChunks.push_back(
                    {chunk.GetChunkIdx(), chunk.GetGeneration()});
                ++*Counters.Chunks.ChunksOwned;
            }
            RestoredIntegrityMapping.GenerationCounter = snapshot.GetGenerationCounter();
        }
        if (const auto it = msg.StartingPoints.find(TLogSignature::SignaturePersistentBufferChunkMap); it != msg.StartingPoints.end()) {
            NPDisk::TLogRecord& record = it->second;
            PersistentBufferChunkMapSnapshotLsn = record.Lsn;

            NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord chunkMap;
            const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
            Y_ABORT_UNLESS(success);
            for (auto idx : chunkMap.GetChunkIdxs()) {
                PersistentBufferChunks.emplace_back(idx);
            }
            PersistentBufferUniqueId = chunkMap.GetUniqueId();
        }
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound));
    }

    void TDDiskActor::Handle(NPDisk::TEvReadLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvReadLogResult)",
            {"marker", "BSDD03"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvReadLogResult)")) {
            return;
        }

        ++*Counters.RecoveryLog.ReadLogChunks;

        for (const NPDisk::TLogRecord& record : msg.Results) {
            switch (record.Signature.GetUnmasked()) {
                case TLogSignature::SignatureDDiskChunkMap:
                    if (ChunkMapSnapshotLsn + 1 <= record.Lsn) {
                        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
                        const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
                        Y_ABORT_UNLESS(success);
                        using TChunkMapLogRecord = NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
                        switch (chunkMap.GetRecordCase()) {
                            case TChunkMapLogRecord::kIncrement: {
                                const auto& increment = chunkMap.GetIncrement();
                                if (increment.HasIntegrityChunk()) {
                                    const auto& chunk = increment.GetIntegrityChunk();
                                    RestoredIntegrityMapping.IntegrityChunks.push_back(
                                        {chunk.GetChunkIdx(), chunk.GetGeneration()});
                                    CommittedIntegrityChunks.push_back(
                                        {chunk.GetChunkIdx(), chunk.GetGeneration()});
                                    ++*Counters.Chunks.ChunksOwned;
                                }
                                const auto& data = increment.GetDataChunk();
                                SetDataChunkMapping(data.GetTabletId(), &Tablets[data.GetTabletId()].ChunkRefs[data.GetVChunkIndex()],
                                    data.GetChunkIdx());
                                ++*Counters.Chunks.ChunksOwned;
                                if (data.HasExtentRef()) {
                                    const auto& ref = data.GetExtentRef();
                                    RestoredIntegrityMapping.Extents.push_back({
                                        .Key = {data.GetTabletId(), data.GetVChunkIndex()},
                                        .DataChunkIdx = data.GetChunkIdx(),
                                        .Ref = {ref.GetIntegrityChunkIdx(), ref.GetExtentSlot(), ref.GetVChunkGeneration()},
                                    });
                                }
                                break;
                            }
                            default:
                                Y_ABORT("unexpected chunk map record case");
                        }
                        ++*Counters.RecoveryLog.LogRecordsApplied;
                    }
                    break;
                case TLogSignature::SignaturePersistentBufferChunkMap:
                    if (record.Lsn > PersistentBufferChunkMapSnapshotLsn) {
                        Y_ABORT("unexpected log signature SignaturePersistentBufferChunkMap");
                    }
                    break;
                default:
                    Y_ABORT("unexpected log signature");
            }
            NextLsn = record.Lsn + 1;
            ++*Counters.RecoveryLog.LogRecordsProcessed;
        }

        if (msg.IsEndOfLog) {
            ValidateChecksumsModeAfterLogReplay();
            ReconcileStartupReservations();
        } else {
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound,
                msg.NextPosition));
        }
    }

    void TDDiskActor::ReconcileStartupReservations() {
        std::vector<TChunkIdx> orphanChunks;
        // Snapshot the complete recovered live set before boot-time integrity reclamation
        // changes it. Failed recovery cannot establish which owned chunks are orphans.
        if (!IsBroken()) {
            absl::flat_hash_set<TChunkIdx> live(PersistentBufferChunks.begin(), PersistentBufferChunks.end());
            for (const auto& [tabletId, tablet] : Tablets) {
                const auto& chunks = tablet.ChunkRefs;
                Y_UNUSED(tabletId);
                for (const auto& [vChunkIndex, ref] : chunks) {
                    Y_UNUSED(vChunkIndex);
                    live.insert(ref.ChunkIdx);
                }
            }
            for (const auto& chunk : RestoredIntegrityMapping.IntegrityChunks) {
                live.insert(chunk.ChunkIdx);
            }
            for (const auto& extent : RestoredIntegrityMapping.Extents) {
                live.insert(extent.Ref.IntegrityChunkIdx);
            }
            std::sort(OwnedChunksOnBoot.begin(), OwnedChunksOnBoot.end());
            OwnedChunksOnBoot.erase(std::unique(OwnedChunksOnBoot.begin(), OwnedChunksOnBoot.end()),
                OwnedChunksOnBoot.end());
            for (const TChunkIdx chunk : OwnedChunksOnBoot) {
                if (!live.contains(chunk)) {
                    orphanChunks.push_back(chunk);
                }
            }
        }
        OwnedChunksOnBoot.clear();
        for (const TChunkIdx chunk : orphanChunks) {
            if (Stopping) {
                co_return;
            }
            auto request = std::make_unique<NPDisk::TEvChunkForget>(PDiskParams->Owner,
                PDiskParams->OwnerRound, TVector<TChunkIdx>{chunk});
            request->IsDDisk = true;
            const ui64 cookie = NActors::AllocateWaitCookie();
            Send(BaseInfo.PDiskActorID, request.release(), IEventHandle::FlagTrackDelivery, cookie);
            auto event = co_await NActors::ActorWaitForEvent<IEventHandle>(cookie);
            if (Stopping) {
                co_return;
            }
            if (event->GetTypeRewrite() == TEvents::TEvUndelivered::EventType) {
                BeginStopping("PDisk startup forget request was not delivered");
                co_return;
            }
            Y_ABORT_UNLESS(event->GetTypeRewrite() == NPDisk::TEvChunkForgetResult::EventType);
            const auto& msg = *event->Get<NPDisk::TEvChunkForgetResult>();
            if (msg.Status == NKikimrProto::ERROR) {
                YDB_LOG_WARN("DDisk startup orphan cleanup rejected; preserving chunk",
                    {"DDiskId", DDiskId}, {"chunk", chunk}, {"reason", msg.ErrorReason});
            } else if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "startup orphan cleanup")) {
                co_return;
            }
        }
        if (!Stopping) {
            FinishRecovery();
        }
    }

    void TDDiskActor::FinishRecovery() {
        if (Config.EnableChecksums && !IsBroken()) {
            // Restore the DataChunk -> IntegrityExtent mapping accumulated from the snapshot and
            // the replayed increments. Metadata images, including their used-block bitmaps,
            // are loaded lazily from the restored extents.
            IntegrityManager->ApplyMappingSnapshot(RestoredIntegrityMapping);
            RestoredIntegrityMapping = {};
            // A durable increment is only logged after formatting, so restored chunks are Ready.
            // Empty integrity chunks (no restored extents) are released here.
            PrepareIntegrityReclamation();
        }
        RestoredIntegrityMapping = {};
        CreatePersistentBuffer();

        LogReplayComplete = true;
        if (DeferredCutLogFreeUpToLsn) {
            const ui64 freeUpToLsn = *DeferredCutLogFreeUpToLsn;
            DeferredCutLogFreeUpToLsn.reset();
            ProcessCutLog(freeUpToLsn);
        }
        StartHandlingQueries();
    }

    void TDDiskActor::CreatePersistentBuffer() {
        auto format = NPDisk::TDiskFormatPtr(new NPDisk::TDiskFormat(*DiskFormat), +[](NPDisk::TDiskFormat* ptr) {
            delete ptr;
        });
        if (PersistentBufferUniqueId == 0) {
            PersistentBufferUniqueId = RandomNumber<ui64>();
        }
        auto pbActor = std::make_unique<TDDiskActor>(TVDiskConfig::TBaseInfo(BaseInfo),
            Info, TPersistentBufferFormat(PersistentBufferFormat), TDDiskConfig(Config), CountersParent,
            PersistentBufferChunks, PersistentBufferUniqueId, PDiskParams, std::move(format)
#if defined(__linux__)
            , UringRouter
#endif
            );
        pbActor->ParentDDiskId = SelfId();
        auto *as = TActivationContext::ActorSystem();
        PersistentBufferActorId = as->Register(pbActor.release(), TMailboxType::Revolving, AppData()->SystemPoolId);
        auto pbServiceId = MakeBlobStoragePersistentBufferId(BaseInfo.PDiskActorID.NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
        as->RegisterLocalService(pbServiceId, PersistentBufferActorId);
        YDB_LOG_DEBUG("TDDiskActor::CreatePersistentBuffer()",
            {"marker", "BSDD03"},
            {"DDiskId", DDiskId},
            {"pbServiceId", pbServiceId},
            {"persistentBufferActorId", PersistentBufferActorId});
    }

    void TDDiskActor::InitUring() {
#if defined(__linux__)
        if (Config.ForcePDiskFallback) {
            UringRouter.reset();
        }
        if (UringRouter) {
            YDB_LOG_INFO("TDDiskActor::InitUring using shared PDisk io_uring",
                {"marker", "BSDD20"},
                {"DDiskId", DDiskId},
                {"config", UringRouter->GetConfig().ToString()});
        }
#endif
    }

    void TDDiskActor::StartHandlingQueries() {
        InitUring();
        TActivationContext::Send(new IEventHandle(TEvPrivate::EvHandleSingleQuery, 0, SelfId(), SelfId(), nullptr, 0));
    }

    void TDDiskActor::HandleSingleQuery() {
        HandlingQueries = true;
        if (!PendingQueries.empty()) {
            auto temp = PendingQueries.front().Release();
            PendingQueries.pop();
            Receive(temp);
            HandlingQueries = false; // to prevent reordering of incoming queries
            StartHandlingQueries();
        }
    }

    ui64 TDDiskActor::GetFirstLsnToKeep() const {
        return std::min(ChunkMapSnapshotLsn, PersistentBufferChunkMapSnapshotLsn);
    }

    void TDDiskActor::IssuePDiskLogRecord(TLogSignature signature, TChunkIdx chunkIdxToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsn,
            TVector<TChunkIdx> chunksToDelete, std::shared_ptr<TLogTicket> ticket)
    {
        TVector<TChunkIdx> chunksToCommit;
        if (chunkIdxToCommit) {
            chunksToCommit.push_back(chunkIdxToCommit);
        }
        IssuePDiskLogRecord(signature, std::move(chunksToCommit), data, startingPointLsn,
            std::move(chunksToDelete), std::move(ticket));
    }

    void TDDiskActor::IssuePDiskLogRecord(TLogSignature signature, TVector<TChunkIdx> chunksToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsn,
            TVector<TChunkIdx> chunksToDelete, std::shared_ptr<TLogTicket> ticket)
    {
        TString buffer;
        const bool success = data.SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);

        const ui64 lsn = NextLsn++;
        if (startingPointLsn) {
            *startingPointLsn = lsn;
        }

        NPDisk::TCommitRecord cr;
        cr.FirstLsnToKeep = startingPointLsn ? GetFirstLsnToKeep() : 0;
        cr.IsStartingPoint = startingPointLsn != nullptr;
        cr.CommitChunks = std::move(chunksToCommit);
        cr.DeleteChunks = std::move(chunksToDelete);

        const ui64 cookie = NextCookie++;
        // Track every LSN, including records without waiters. Detach the complete reply
        // batch before waking a coroutine, which may submit more log records.
        Y_ABORT_UNLESS(LogWaiters.emplace(lsn, TLogWaiter{
            .DeliveryCookie = cookie,
            .IsDDisk = signature == TLogSignature::SignatureDDiskChunkMap,
            .Ticket = std::move(ticket),
        }).second);
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvLog(PDiskParams->Owner, PDiskParams->OwnerRound, signature, cr,
            TRcBuf(std::move(buffer)), {lsn, lsn}, nullptr, TWriteSource::DDiskBoot),
            IEventHandle::FlagTrackDelivery, cookie);
    }

    void TDDiskActor::CompleteLogTicket(const std::shared_ptr<TLogTicket>& ticket, bool ok) {
        if (!ticket || ticket->Result) {
            return;
        }
        ticket->Result = ok;
        ticket->Changed.NotifyAll();
    }

    void TDDiskActor::FailLogWaiters(bool ddiskOnly) {
        std::vector<std::shared_ptr<TLogTicket>> tickets;
        for (auto it = LogWaiters.begin(); it != LogWaiters.end(); ) {
            if (!ddiskOnly || it->second.IsDDisk) {
                tickets.push_back(std::move(it->second.Ticket));
                LogWaiters.erase(it++);
            } else {
                ++it;
            }
        }
        for (auto& ticket : tickets) {
            CompleteLogTicket(ticket, false);
        }
    }

    void TDDiskActor::Handle(NPDisk::TEvLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvLogResult)",
            {"marker", "BSDD05"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        std::vector<std::shared_ptr<TLogTicket>> tickets;
        for (const auto& result : msg.Results) {
            auto it = LogWaiters.find(result.Lsn);
            if (it == LogWaiters.end()) {
                continue;
            }
            tickets.push_back(std::move(it->second.Ticket));
            LogWaiters.erase(it);
        }
        if (tickets.empty() && !msg.Results.empty()) {
            return; // duplicate or retired LSN, including a stale failure status
        }
        const bool statusOk = CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvLogResult)");
        for (auto& ticket : tickets) {
            CompleteLogTicket(ticket, statusOk && !Stopping);
            if (statusOk) {
                ++*Counters.RecoveryLog.LogRecordsWritten;
            }
        }
    }

} // NKikimr::NDDisk
