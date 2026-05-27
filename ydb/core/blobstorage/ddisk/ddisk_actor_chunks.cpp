#include "ddisk_actor.h"

#include <util/generic/overloaded.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        ChunkAllocateQueue.emplace(TChunkForData{tabletId, vChunkIndex});
        HandleChunkReserved();
    }

    void TDDiskActor::Handle(TEvPrivate::TEvIssuePersistentBufferChunkAllocation::TPtr ev) {
        if (!CanHandleQuery(ev)) {
            return;
        }
        if (!IssuePersistentBufferChunkAllocationInflight) {
            IssuePersistentBufferChunkAllocationInflight = true;
            ChunkAllocateQueue.emplace(TChunkForPersistentBuffer{});
            HandleChunkReserved();
        }
    }

    void TDDiskActor::Handle(NPDisk::TEvChunkReserveResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD04, "TDDiskActor::Handle(TEvChunkReserveResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        Y_ABORT_UNLESS(ReserveInFlight);
        ReserveInFlight = false;

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvChunkReserveResult)")) {
            return;
        }

        for (TChunkIdx chunkIdx : msg.ChunkIds) {
            ChunkReserve.push(chunkIdx);
        }

        HandleChunkReserved();
    }

    void TDDiskActor::HandleChunkReserved() {
        Y_ABORT_UNLESS(!IsPersistentBufferActor);
        while (!ChunkAllocateQueue.empty() && !ChunkReserve.empty()) {
            const auto chunkAllocate = ChunkAllocateQueue.front();
            ChunkAllocateQueue.pop();
            const TChunkIdx chunkIdx = ChunkReserve.front();
            ChunkReserve.pop();
            std::visit(TOverloaded{
                [this, chunkIdx](const TChunkForData& data) {
                    const auto tabletId = data.TabletId;
                    const auto vChunkIndex = data.VChunkIndex;
                    Y_ABORT_UNLESS(ChunkRefs.contains(tabletId) && ChunkRefs[tabletId].contains(vChunkIndex));

                    ChunkMapIncrementsInFlight.emplace(tabletId, vChunkIndex, chunkIdx);

                    IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, chunkIdx, CreateChunkMapIncrement(
                            tabletId, vChunkIndex, chunkIdx), nullptr, [this, tabletId, vChunkIndex, chunkIdx] {
                        TChunkRef& chunkRef = ChunkRefs[tabletId][vChunkIndex];
                        Y_ABORT_UNLESS(!chunkRef.ChunkIdx);
                        chunkRef.ChunkIdx = chunkIdx;

                        if (!chunkRef.PendingEventsForChunk.empty()) {
                            Send(SelfId(), new TEvPrivate::TEvHandleEventForChunk(tabletId, vChunkIndex));
                        }

                        const size_t numErased = ChunkMapIncrementsInFlight.erase({tabletId, vChunkIndex, chunkIdx});
                        Y_ABORT_UNLESS(numErased == 1);
                        ++*Counters.Chunks.ChunksOwned;
                    });
                    if (ChunkMapSnapshotLsn == Max<ui64>()) {
                        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(),
                            &ChunkMapSnapshotLsn, {});
                    }
                },
                [this, chunkIdx](const TChunkForPersistentBuffer&) {
                    Y_DEBUG_ABORT_UNLESS(std::find(PersistentBufferChunks.begin(),
                    PersistentBufferChunks.end(), chunkIdx) == PersistentBufferChunks.end());
                    PersistentBufferChunks.emplace_back(chunkIdx);
                    IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, chunkIdx
                        , CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn, [this, chunkIdx] {
                        IssuePersistentBufferChunkAllocationInflight = false;
                        Send(PersistentBufferActorId, new TEvPrivate::TEvHandlePersistentBufferEventForChunk(chunkIdx));
                        ++*Counters.Chunks.ChunksOwned;
                    });
                }
            }, chunkAllocate);
        }
        if (ChunkReserve.size() < MinChunksReserved && !ReserveInFlight) { // ask for another reservation
            STLOG(PRI_DEBUG, BS_DDISK, BSDD28,
                "TDDiskActor::HandleChunkReserved requesting chunk reserve",
                (DDiskId, DDiskId),
                (ChunkReserveSize, ChunkReserve.size()),
                (MinChunksReserved, MinChunksReserved),
                (RequestCount, MinChunksReserved - ChunkReserve.size()));
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound,
                MinChunksReserved - ChunkReserve.size()));
            ReserveInFlight = true;
        }
    }

    void TDDiskActor::Handle(TEvPrivate::TEvHandleEventForChunk::TPtr ev) {
        auto& msg = *ev->Get();
        TChunkRef& chunkRef = ChunkRefs[msg.TabletId][msg.VChunkIndex];

        // temporarily remove queue to unblock execution of queries for this chunk
        std::queue<TPendingEvent> queue;
        queue.swap(chunkRef.PendingEventsForChunk);

        // handle front event
        Y_ABORT_UNLESS(!queue.empty());
        auto temp = queue.front().Release();
        queue.pop();
        Receive(temp);

        // schedule processing another one, if needed
        if (!queue.empty()) {
            TActivationContext::Send(ev.Release());
        }

        // put queue back in
        queue.swap(chunkRef.PendingEventsForChunk);
        Y_ABORT_UNLESS(queue.empty()); // ensure nothing more appeared during event handling
    }

    void TDDiskActor::Handle(NPDisk::TEvCutLog::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD06, "TDDiskActor::Handle(TEvCutLog)", (DDiskId, DDiskId), (Msg, msg));

        if (ChunkMapSnapshotLsn < msg.FreeUpToLsn) { // we have to rewrite snapshot
            IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(), &ChunkMapSnapshotLsn, {});
        }
        if (PersistentBufferChunkMapSnapshotLsn < msg.FreeUpToLsn) { // we have to rewrite snapshot
            IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, 0, CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn, {});
        }
        ++*Counters.RecoveryLog.CutLogMessages;
    }

    NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord TDDiskActor::CreatePersistentBufferChunkMapSnapshot() {
        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord record;
        for (const ui32 chunkIdx : PersistentBufferChunks) {
            record.AddChunkIdxs(chunkIdx);
        }
        record.SetUniqueId(PersistentBufferUniqueId);
        Y_ABORT_UNLESS(PersistentBufferUniqueId != 0);
        return record;
    }

    NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord TDDiskActor::CreateChunkMapSnapshot() {
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
        auto *snapshot = record.MutableSnapshot();

        for (const auto& [tabletId, chunks] : ChunkRefs) {
            auto *tabletRecord = snapshot->AddTabletRecords();
            tabletRecord->SetTabletId(tabletId);

            for (const auto& [vChunkIndex, chunkRef] : chunks) {
                auto *item = tabletRecord->AddChunkRefs();
                item->SetVChunkIndex(vChunkIndex);
                item->SetChunkIdx(chunkRef.ChunkIdx);
            }

            // check for increments in flight, they would have been committed by the time this entry gets read
            for (auto it = ChunkMapIncrementsInFlight.lower_bound({tabletId, 0, 0});
                    it != ChunkMapIncrementsInFlight.end() && std::get<0>(*it) == tabletId; ++it) {
                const auto& [tabletId, vChunkIndex, chunkIdx] = *it;
                auto *item = tabletRecord->AddChunkRefs();
                item->SetVChunkIndex(vChunkIndex);
                item->SetChunkIdx(chunkIdx);
            }
        }

        ++*Counters.RecoveryLog.NumChunkMapSnapshots;
        return record;
    }

    NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord TDDiskActor::CreateChunkMapIncrement(ui64 tabletId,
            ui64 vChunkIndex, TChunkIdx chunkIdx) {
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
        auto *increment = record.MutableIncrement();

        increment->SetTabletId(tabletId);
        increment->SetVChunkIndex(vChunkIndex);
        increment->SetChunkIdx(chunkIdx);

        ++*Counters.RecoveryLog.NumChunkMapIncrements;
        return record;
    }

    void TDDiskActor::Handle(TEvDeleteTabletChunks::TPtr ev) {
        if (!CheckQuery(*ev, nullptr)) {
            return;
        }

        const TQueryCredentials creds(ev->Get()->Record.GetCredentials());
        const ui64 tabletId = creds.TabletId;

        STLOG(PRI_DEBUG, BS_DDISK, BSDD51, "TDDiskActor::Handle(TEvDeleteTabletChunks)",
            (DDiskId, DDiskId), (TabletId, tabletId));

        // Reject if any chunk allocation for this tablet is in flight (log record pending)
        {
            auto it = ChunkMapIncrementsInFlight.lower_bound({tabletId, 0, 0});
            if (it != ChunkMapIncrementsInFlight.end() && std::get<0>(*it) == tabletId) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "chunk allocation is in flight for tablet"));
                return;
            }
        }

        const auto tabletIt = ChunkRefs.find(tabletId);

        if (tabletIt == ChunkRefs.end()) {
            // tablet has no chunks
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            return;
        }

        // Reject if any VChunk has a pending event queue (allocation queued but not yet in log)
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second) {
            if (!chunkRef.PendingEventsForChunk.empty()) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "chunk allocation is queued for tablet"));
                return;
            }
        }

        // Collect physical chunk IDs and erase the tablet from the in-memory chunk map
        TVector<TChunkIdx> chunksToDelete;
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second) {
            if (chunkRef.ChunkIdx) {
                chunksToDelete.push_back(chunkRef.ChunkIdx);
            }
        }
        ChunkRefs.erase(tabletIt);

        if (chunksToDelete.empty()) {
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            return;
        }

        *Counters.Chunks.ChunksOwned -= chunksToDelete.size();

        // Capture reply info before issuing the async log record
        const TActorId replyTo = ev->Sender;
        const ui64 replyCookie = ev->Cookie;
        const TActorId replySession = ev->InterconnectSession;

        // Write a snapshot (without the freed tablet chunks) to the PDisk log.
        // The DeleteChunks field in the commit record tells PDisk to release the physical chunks.
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(),
            &ChunkMapSnapshotLsn,
            [this, replyTo, replyCookie, replySession]() {
                auto h = std::make_unique<IEventHandle>(replyTo, SelfId(),
                    new TEvDeleteTabletChunksResult(NKikimrBlobStorage::NDDisk::TReplyStatus::OK),
                    0, replyCookie);
                if (replySession) {
                    h->Rewrite(TEvInterconnect::EvForward, replySession);
                }
                TActivationContext::Send(h.release());
            },
            std::move(chunksToDelete));
    }

} // NKikimr::NDDisk
