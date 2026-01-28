#include "ddisk_actor.h"
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        ChunkAllocateQueue.emplace(tabletId, vChunkIndex);
        if (!ChunkReserve.empty()) {
            HandleChunkReserved();
        } else if (!ReserveInFlight) {
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound, 5));
            ReserveInFlight = true;
        }
    }

    void TDDiskActor::Handle(NPDisk::TEvChunkReserveResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD04, "TDDiskActor::Handle(TEvChunkReserveResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        Y_ABORT_UNLESS(ReserveInFlight);
        ReserveInFlight = false;

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        for (TChunkIdx chunkIdx : msg.ChunkIds) {
            ChunkReserve.push(chunkIdx);
        }

        HandleChunkReserved();
    }

    void TDDiskActor::HandleChunkReserved() {
        Y_ABORT_UNLESS(!ReserveInFlight);

        while (!ChunkAllocateQueue.empty() && !ChunkReserve.empty()) {
            const auto [tabletId, vChunkIndex] = ChunkAllocateQueue.front();
            ChunkAllocateQueue.pop();

            const TChunkIdx chunkIdx = ChunkReserve.front();
            ChunkReserve.pop();

            Y_ABORT_UNLESS(ChunkRefs.contains(tabletId) && ChunkRefs[tabletId].contains(vChunkIndex));

            IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, chunkIdx, CreateChunkMapIncrement(
                    tabletId, vChunkIndex, chunkIdx), nullptr, [this, tabletId, vChunkIndex, chunkIdx] {
                TChunkRef& chunkRef = ChunkRefs[tabletId][vChunkIndex];
                chunkRef.ChunkIdx = chunkIdx;

                if (!chunkRef.PendingEventsForChunk.empty()) {
                    Send(SelfId(), new TEvPrivate::TEvHandleEventForChunk(tabletId, vChunkIndex));
                }

                const size_t numErased = ChunkMapIncrementsInFlight.erase({tabletId, vChunkIndex, chunkIdx});
                Y_ABORT_UNLESS(numErased == 1);

                ++*Counters.Chunks.ChunksOwned;
            });

            ChunkMapIncrementsInFlight.emplace(tabletId, vChunkIndex, chunkIdx);
        }

        if (!ChunkAllocateQueue.empty()) { // ask for another reservation
            Y_ABORT_UNLESS(ChunkReserve.empty());
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound, 5));
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

        ++*Counters.RecoveryLog.CutLogMessages;
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

} // NKikimr::NDDisk
