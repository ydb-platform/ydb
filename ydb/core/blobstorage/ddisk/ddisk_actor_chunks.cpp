#include "ddisk_actor.h"

#include <util/generic/overloaded.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        ChunkAllocateQueue.emplace(TChunkForData{tabletId, vChunkIndex});
        HandleChunkReserved();
    }

    void TDDiskActor::IssuePersistentBufferChunkAllocation() {
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

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        for (TChunkIdx chunkIdx : msg.ChunkIds) {
            ChunkReserve.push(chunkIdx);
        }

        HandleChunkReserved();
    }

    void TDDiskActor::HandleChunkReserved() {
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

                    ChunkMapIncrementsInFlight.emplace(tabletId, vChunkIndex, chunkIdx);
                },
                [this, chunkIdx](const TChunkForPersistentBuffer&) {
                    IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, chunkIdx
                        , CreatePersistentBufferChunkMapSnapshot({chunkIdx}), &PersistentBufferChunkMapSnapshotLsn, [this, chunkIdx] {
                        IssuePersistentBufferChunkAllocationInflight = false;
                        Send(SelfId(), new TEvPrivate::TEvHandlePersistentBufferEventForChunk(chunkIdx));
                        ++*Counters.Chunks.ChunksOwned;
                    });
                }
            }, chunkAllocate);
        }
        if (ChunkReserve.size() < MinChunksReserved && !ReserveInFlight) { // ask for another reservation
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

    NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord TDDiskActor::CreatePersistentBufferChunkMapSnapshot(const std::vector<ui64>& newChunkIdxs) {
        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord record;
        for (const auto& chunkIdx : PersistentBufferSpaceAllocator.OwnedChunks) {
            record.AddChunkIdxs(chunkIdx);
        }
        for (const auto& chunkIdx : newChunkIdxs) {
            Y_DEBUG_ABORT_UNLESS(std::find(PersistentBufferSpaceAllocator.OwnedChunks.begin(),
                PersistentBufferSpaceAllocator.OwnedChunks.end(), chunkIdx) == PersistentBufferSpaceAllocator.OwnedChunks.end());
            record.AddChunkIdxs(chunkIdx);
        }
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

} // NKikimr::NDDisk
