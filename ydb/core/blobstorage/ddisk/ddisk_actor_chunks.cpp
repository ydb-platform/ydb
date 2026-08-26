#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <algorithm>
#include <util/generic/overloaded.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/interconnect.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    void TDDiskActor::IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }
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

    void TDDiskActor::Handle(TEvPrivate::TEvDeallocatePersistentBufferChunk::TPtr ev) {
        auto chunkIdx = ev->Get()->ChunkIdx;
        auto it = std::find(PersistentBufferChunks.begin(), PersistentBufferChunks.end(), chunkIdx);
        Y_DEBUG_ABORT_UNLESS(it != PersistentBufferChunks.end());
        PersistentBufferChunks.erase(it);
        IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, 0
            , CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn, [this, chunkIdx] {
            Send(PersistentBufferActorId, new TEvPrivate::TEvDeallocatePersistentBufferChunkResult(chunkIdx));
            --*Counters.Chunks.ChunksOwned;
        }, {chunkIdx});
    }

    void TDDiskActor::Handle(NPDisk::TEvChunkReserveResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvChunkReserveResult)",
            {"marker", "BSDD04"},
            {"DDiskId", DDiskId},
            {"msg", msg});

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
            if (Y_UNLIKELY(IsBroken())
                    && !std::holds_alternative<TChunkForPersistentBuffer>(ChunkAllocateQueue.front())) {
                ChunkAllocateQueue.pop();
                continue;
            }
            const auto chunkAllocate = ChunkAllocateQueue.front();
            ChunkAllocateQueue.pop();
            const TChunkIdx chunkIdx = ChunkReserve.front();
            ChunkReserve.pop();
            std::visit(TOverloaded{
                [this, chunkIdx](const TChunkForData& data) {
                    const auto tabletId = data.TabletId;
                    const auto vChunkIndex = data.VChunkIndex;
                    Y_ABORT_UNLESS(ChunkRefs.contains(tabletId) && ChunkRefs[tabletId].contains(vChunkIndex));

                    const bool inserted = DataChunkAllocationsInFlight.try_emplace(
                        std::make_pair(tabletId, vChunkIndex), TDataChunkAllocationInFlight{.ChunkIdx = chunkIdx}).second;
                    Y_ABORT_UNLESS(inserted);

                    IntegrityManager->OnDataChunkAllocated({tabletId, vChunkIndex}, chunkIdx);
                    DrainIntegrityManager(/*kickReserve=*/ false);
                },
                [this, chunkIdx](const TChunkForIntegrity&) {
                    // Demand may have vanished since this allocation was queued (a tablet deletion
                    // can free enough slots): return the chunk to the reserve instead of formatting it.
                    if (IntegrityManager->CancelChunkAllocationIfExcess()) {
                        ChunkReserve.push(chunkIdx);
                        return;
                    }

                    IntegrityManager->OnIntegrityChunkAllocated(chunkIdx);
                    DrainIntegrityManager(/*kickReserve=*/ false);
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
            // Chunk-map increments (data and integrity alike) need a snapshot starting point to
            // replay from.
            if (!std::holds_alternative<TChunkForPersistentBuffer>(chunkAllocate)
                    && ChunkMapSnapshotLsn == Max<ui64>()) {
                IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(),
                    &ChunkMapSnapshotLsn, {});
            }
        }
        if (ChunkReserve.size() < MinChunksReserved && !ReserveInFlight) { // ask for another reservation
            YDB_LOG_DEBUG("TDDiskActor::HandleChunkReserved requesting chunk reserve",
                {"marker", "BSDD28"},
                {"DDiskId", DDiskId},
                {"chunkReserveSize", ChunkReserve.size()},
                {"minChunksReserved", MinChunksReserved},
                {"requestCount", MinChunksReserved - ChunkReserve.size()});
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound,
                MinChunksReserved - ChunkReserve.size()));
            ReserveInFlight = true;
        }
    }

    bool TDDiskActor::ProcessIntegrityActions() {
        if (Y_UNLIKELY(IsBroken())) {
            Y_UNUSED(IntegrityManager->TakeActions());
            return false;
        }

        bool queuedChunkAllocation = false;
        for (auto& action : IntegrityManager->TakeActions()) {
            std::visit(TOverloaded{
                [&](TIntegrityManager::TAllocateIntegrityChunk&) {
                    ChunkAllocateQueue.emplace(TChunkForIntegrity{});
                    queuedChunkAllocation = true;
                },
                [&](TIntegrityManager::TWriteIo& io) {
                    if (io.Kind == TIntegrityManager::EWriteIoKind::Pair) {
                        Counters.Checksums.IntegrityPairWrites->Inc();
                    }
                    std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TIntegrityIoOp>();
                    static_cast<TIntegrityIoOp*>(op.get())->SetIoId(io.IoId);

                    const ui64 diskOffset = DiskFormat->Offset(io.ChunkIdx, 0, io.OffsetInBytes);
                    op->PrepareWrite(TRope(std::move(io.Data)), diskOffset, io.ChunkIdx, io.OffsetInBytes);
                    DirectUringOp(op);
                },
                [&](TIntegrityManager::TReadIo& io) {
                    Counters.Checksums.IntegrityPairReads->Inc();
                    std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TIntegrityIoOp>();
                    static_cast<TIntegrityIoOp*>(op.get())->SetIoId(io.IoId);

                    const ui64 diskOffset = DiskFormat->Offset(io.ChunkIdx, 0, io.OffsetInBytes);
                    op->PrepareRead(io.Size, diskOffset, io.ChunkIdx, io.OffsetInBytes);
                    DirectUringOp(op);
                },
            }, action);
        }
        return queuedChunkAllocation;
    }

    void TDDiskActor::ProcessIntegrityCompletions() {
        for (auto& result : IntegrityManager->TakeCompletedOperations()) {
            if (result.Status == TIntegrityManager::EOperationStatus::Corrupted) {
                Counters.Checksums.IntegrityCorruption->Inc();
                if (result.LostWriteDetected) {
                    Counters.Checksums.IntegrityLostWriteDetected->Inc();
                }
            }
            if (result.Kind == TIntegrityManager::EOperationKind::Write) {
                const auto writeIt = PendingClientWrites.find(result.OperationId);
                if (writeIt != PendingClientWrites.end()) {
                    writeIt->second.IntegrityCompleted = true;
                    writeIt->second.IntegrityStatus = result.Status;
                    writeIt->second.IntegrityError = std::move(result.ErrorReason);
                    MaybeFinishClientWrite(result.OperationId);
                    continue;
                }

                const auto syncIt = PendingSyncSegments.find(result.OperationId);
                if (syncIt != PendingSyncSegments.end()) {
                    syncIt->second.IntegrityCompleted = true;
                    syncIt->second.IntegrityStatus = result.Status;
                    syncIt->second.IntegrityError = std::move(result.ErrorReason);
                    MaybeFinishSyncSegment(result.OperationId);
                }
                continue;
            }

            const auto readIt = PendingChecksumReads.find(result.OperationId);
            if (readIt == PendingChecksumReads.end()) {
                continue;
            }
            std::unique_ptr<IEventHandle> readEvent = std::move(readIt->second.Event);
            PendingChecksumReads.erase(readIt);
            if (result.Status == TIntegrityManager::EOperationStatus::Corrupted) {
                const TBlockSelector selector(readEvent->Get<TEvRead>()->Record.GetSelector());
                Counters.Interface.Read.Reply(false, selector.Size);
                SendReply(*readEvent, std::make_unique<TEvReadResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::CORRUPTED, result.ErrorReason));
            } else {
                StartDDiskDataRead(std::move(readEvent), std::move(result.Checksums));
            }
        }
    }

    void TDDiskActor::DrainIntegrityManager(bool kickReserve) {
        const bool queuedChunkAllocation = ProcessIntegrityActions();
        ProcessIntegrityCompletions();
        OpenDataChunkWritePath(IntegrityManager->TakePlacedKeys());
        if (kickReserve && queuedChunkAllocation) {
            HandleChunkReserved();
        }
    }

    void TDDiskActor::OpenDataChunkWritePath(std::vector<TIntegrityManager::TDataChunkKey> placedKeys) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }
        for (const auto& key : placedKeys) {
            const auto it = DataChunkAllocationsInFlight.find({key.TabletId, key.VChunkIndex});
            Y_ABORT_UNLESS(it != DataChunkAllocationsInFlight.end());
            TChunkRef& chunkRef = ChunkRefs[key.TabletId][key.VChunkIndex];
            if (!chunkRef.ChunkIdx) {
                chunkRef.ChunkIdx = it->second.ChunkIdx;
            }
            Y_ABORT_UNLESS(chunkRef.ChunkIdx == it->second.ChunkIdx);
            if (!chunkRef.PendingEventsForChunk.empty()) {
                Send(SelfId(), new TEvPrivate::TEvHandleEventForChunk(key.TabletId, key.VChunkIndex));
            }
        }
    }

    bool TDDiskActor::IsIntegrityChunkCommitted(TChunkIdx chunkIdx) const {
        return std::any_of(CommittedIntegrityChunks.begin(), CommittedIntegrityChunks.end(),
            [chunkIdx](const auto& entry) { return entry.ChunkIdx == chunkIdx; });
    }

    void TDDiskActor::ReclaimUnusedIntegrityChunks(std::function<void()> completion) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        const auto releasableChunks = IntegrityManager->TakeReleasableIntegrityChunks();
        DrainIntegrityManager();

        TVector<TChunkIdx> chunksToDelete;
        for (const TChunkIdx chunkIdx : releasableChunks) {
            if (IsIntegrityChunkCommitted(chunkIdx)) {
                const size_t erased = std::erase_if(CommittedIntegrityChunks, [chunkIdx](const auto& entry) {
                    return entry.ChunkIdx == chunkIdx;
                });
                Y_ABORT_UNLESS(erased == 1);
                chunksToDelete.push_back(chunkIdx);
            } else {
                ChunkReserve.push(chunkIdx);
            }
        }

        if (chunksToDelete.empty()) {
            if (completion) {
                completion();
            }
            return;
        }

        *Counters.Chunks.ChunksOwned -= chunksToDelete.size();
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, TChunkIdx(0), CreateChunkMapSnapshot(),
            &ChunkMapSnapshotLsn, std::move(completion), std::move(chunksToDelete));
    }

    void TDDiskActor::IssueDataChunkIncrement(ui64 tabletId, ui64 vChunkIndex) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        const auto it = DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
        Y_ABORT_UNLESS(it != DataChunkAllocationsInFlight.end());
        auto& allocation = it->second;
        if (allocation.LogIssued) {
            return;
        }
        Y_ABORT_UNLESS(IntegrityManager->IsExtentReady({tabletId, vChunkIndex}));
        const auto* ref = IntegrityManager->FindExtentRef({tabletId, vChunkIndex});
        Y_ABORT_UNLESS(ref);

        allocation.LogIssued = true;
        const TChunkIdx chunkIdx = allocation.ChunkIdx;
        ChunkMapIncrementsInFlight.emplace(tabletId, vChunkIndex, chunkIdx);

        TVector<TChunkIdx> commitChunks;
        const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk = nullptr;
        TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry integrityEntry;
        if (!IsIntegrityChunkCommitted(ref->IntegrityChunkIdx)) {
            integrityEntry = {
                .ChunkIdx = ref->IntegrityChunkIdx,
                .Generation = IntegrityManager->GetIntegrityChunkGeneration(ref->IntegrityChunkIdx),
            };
            CommittedIntegrityChunks.push_back(integrityEntry);
            integrityChunk = &CommittedIntegrityChunks.back();
            commitChunks.push_back(ref->IntegrityChunkIdx);
        }
        commitChunks.push_back(chunkIdx);
        allocation.NewlyCommittedChunks = commitChunks.size();

        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, std::move(commitChunks),
            CreateChunkMapIncrement(tabletId, vChunkIndex, chunkIdx, *ref, integrityChunk),
            nullptr, [this, tabletId, vChunkIndex] {
                CompleteDataChunkAllocation(tabletId, vChunkIndex);
            });
    }

    void TDDiskActor::FlushParkedAllocationReplies(TDataChunkAllocationInFlight& allocation) {
        for (auto& parked : allocation.ParkedWriteResults) {
            const bool isOk = parked.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            std::optional<TString> errorReason;
            if (parked.ErrorMessage) {
                errorReason.emplace(std::move(parked.ErrorMessage));
            }
            auto reply = std::make_unique<TEvWriteResult>(parked.Status, errorReason);
            Counters.Interface.Write.Reply(isOk, parked.TotalSize, parked.RequestTimeMs);
            auto h = std::make_unique<IEventHandle>(parked.OriginalRequester, SelfId(), reply.release(),
                0, parked.Cookie, nullptr, parked.Span.GetTraceId());
            if (parked.InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, parked.InterconnectSession);
            }
            parked.Span.End();
            TActivationContext::Send(h.release());
        }
        allocation.ParkedWriteResults.clear();

        auto parkedSyncIds = std::exchange(allocation.ParkedSyncIds, {});
        for (const ui64 syncId : parkedSyncIds) {
            const auto syncIt = SyncsInFlight.find(syncId);
            if (syncIt != SyncsInFlight.end()) {
                ReplySync(syncIt);
            }
        }
    }

    void TDDiskActor::CompleteDataChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        const auto it = DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
        Y_ABORT_UNLESS(it != DataChunkAllocationsInFlight.end());
        auto allocation = std::move(it->second);
        DataChunkAllocationsInFlight.erase(it);

        TChunkRef& chunkRef = ChunkRefs[tabletId][vChunkIndex];
        Y_ABORT_UNLESS(chunkRef.ChunkIdx == allocation.ChunkIdx);

        const size_t numErased = ChunkMapIncrementsInFlight.erase({tabletId, vChunkIndex, allocation.ChunkIdx});
        Y_ABORT_UNLESS(numErased == 1);
        *Counters.Chunks.ChunksOwned += allocation.NewlyCommittedChunks;

        FlushParkedAllocationReplies(allocation);
    }

    void TDDiskActor::Handle(TEvPrivate::TEvIntegrityIoResult::TPtr ev) {
        auto& msg = *ev->Get();

        // Transient OVERLOADED errors are retried in TDirectIoOpBase::OnComplete while the
        // op still owns its buffers. Any non-OK status that reaches this handler is fatal.
        if (msg.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            EnterBroken(msg.ErrorMessage);
            return;
        }
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        std::vector<TIntegrityManager::TDataChunkKey> readyKeys;
        if (msg.IsRead) {
            IntegrityManager->OnReadIoCompleted(msg.IoId, std::move(msg.Data));
        } else {
            readyKeys = IntegrityManager->OnIoCompleted(msg.IoId);
        }
        DrainIntegrityManager();

        for (const auto& key : readyKeys) {
            IssueDataChunkIncrement(key.TabletId, key.VChunkIndex);
        }

        ReclaimUnusedIntegrityChunks();
    }

    void TDDiskActor::Handle(TEvPrivate::TEvHandleEventForChunk::TPtr ev) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

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

        // Receive may synchronously enter Broken while the remaining events are temporarily
        // outside chunkRef.PendingEventsForChunk, so EnterBroken cannot see and fail them.
        if (Y_UNLIKELY(IsBroken())) {
            while (!queue.empty()) {
                auto pending = queue.front().Release();
                queue.pop();
                FailPendingDDiskQuery(std::unique_ptr<IEventHandle>(pending.Release()));
            }
            return;
        }

        // schedule processing another one, if needed
        if (!queue.empty()) {
            TActivationContext::Send(ev.Release());
        }

        // put queue back in
        queue.swap(chunkRef.PendingEventsForChunk);
        Y_ABORT_UNLESS(queue.empty()); // ensure nothing more appeared during event handling
    }

    void TDDiskActor::ScheduleSerializedWrite(ui64 tabletId, ui64 vChunkIndex) {
        TChunkRef& chunkRef = ChunkRefs.at(tabletId).at(vChunkIndex);
        if (Y_UNLIKELY(IsBroken()) || chunkRef.IntegrityExtentWriteInFlight
                || chunkRef.SerializedWriteResumeScheduled
                || chunkRef.PendingSerializedWrites.empty()) {
            return;
        }
        chunkRef.SerializedWriteResumeScheduled = true;
        Send(SelfId(), new TEvPrivate::TEvHandleSerializedWriteForChunk(tabletId, vChunkIndex));
    }

    void TDDiskActor::ReleaseIntegrityExtentWrite(ui64 tabletId, ui64 vChunkIndex) {
        TChunkRef& chunkRef = ChunkRefs.at(tabletId).at(vChunkIndex);
        Y_ABORT_UNLESS(chunkRef.IntegrityExtentWriteInFlight);
        chunkRef.IntegrityExtentWriteInFlight = false;
        ScheduleSerializedWrite(tabletId, vChunkIndex);
    }

    void TDDiskActor::Handle(TEvPrivate::TEvHandleSerializedWriteForChunk::TPtr ev) {
        auto& msg = *ev->Get();
        // EnterBroken clears SerializedWriteResumeScheduled but cannot recall a
        // self-message already in the mailbox. Bail out before the flag assert.
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        TChunkRef& chunkRef = ChunkRefs.at(msg.TabletId).at(msg.VChunkIndex);
        Y_ABORT_UNLESS(chunkRef.SerializedWriteResumeScheduled);
        chunkRef.SerializedWriteResumeScheduled = false;

        if (chunkRef.IntegrityExtentWriteInFlight
                || chunkRef.PendingSerializedWrites.empty()) {
            return;
        }

        auto pending = chunkRef.PendingSerializedWrites.front().Release();
        chunkRef.PendingSerializedWrites.pop();
        Receive(pending);

        // The resumed event may have become obsolete while parked and therefore not acquire the
        // extent. Continue the FIFO in that case.
        ScheduleSerializedWrite(msg.TabletId, msg.VChunkIndex);
    }

    void TDDiskActor::Handle(NPDisk::TEvCutLog::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvCutLog)",
            {"marker", "BSDD06"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        ++*Counters.RecoveryLog.CutLogMessages;

        // YardInit installs the CutLog recipient before chunk-map replay is complete. Until
        // ApplyMappingSnapshot runs, ChunkRefs may already contain restored data chunks while the
        // integrity manager is still empty, so a snapshot here would either abort or omit replayed
        // mappings. Coalesce early requests and process the strongest one after recovery.
        if (!LogReplayComplete) {
            DeferredCutLogFreeUpToLsn = Max(DeferredCutLogFreeUpToLsn.value_or(0), msg.FreeUpToLsn);
            return;
        }

        ProcessCutLog(msg.FreeUpToLsn);
    }

    void TDDiskActor::ProcessCutLog(ui64 freeUpToLsn) {
        Y_ABORT_UNLESS(LogReplayComplete);

        if (!IsBroken() && ChunkMapSnapshotLsn < freeUpToLsn) { // we have to rewrite snapshot
            IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(), &ChunkMapSnapshotLsn, {});
        }
        if (PersistentBufferChunkMapSnapshotLsn < freeUpToLsn) { // we have to rewrite snapshot
            IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, 0, CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn, {});
        }
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

        const auto fillExtentRef = [this](auto *item, ui64 tabletId, ui64 vChunkIndex) {
            // Non-null for every chunk with a log record: the extent is Ready by the time its
            // increment is issued, and refs survive until the chunk is deleted.
            const auto *ref = IntegrityManager->FindExtentRef({tabletId, vChunkIndex});
            Y_ABORT_UNLESS(ref);
            auto *extentRef = item->MutableExtentRef();
            extentRef->SetIntegrityChunkIdx(ref->IntegrityChunkIdx);
            extentRef->SetExtentSlot(ref->ExtentSlot);
            extentRef->SetVChunkGeneration(ref->VChunkGeneration);
        };

        for (const auto& [tabletId, chunks] : ChunkRefs) {
            auto *tabletRecord = snapshot->AddTabletRecords();
            tabletRecord->SetTabletId(tabletId);

            for (const auto& [vChunkIndex, chunkRef] : chunks) {
                if (!chunkRef.ChunkIdx) {
                    continue;
                }
                if (DataChunkAllocationsInFlight.contains({tabletId, vChunkIndex})) {
                    // Not yet logged: issued increments are covered by ChunkMapIncrementsInFlight.
                    continue;
                }
                auto *item = tabletRecord->AddChunkRefs();
                item->SetVChunkIndex(vChunkIndex);
                item->SetChunkIdx(chunkRef.ChunkIdx);
                fillExtentRef(item, tabletId, vChunkIndex);
            }

            // check for increments in flight, they would have been committed by the time this entry gets read
            for (auto it = ChunkMapIncrementsInFlight.lower_bound({tabletId, 0, 0});
                    it != ChunkMapIncrementsInFlight.end() && std::get<0>(*it) == tabletId; ++it) {
                const auto& [tabletId, vChunkIndex, chunkIdx] = *it;
                auto *item = tabletRecord->AddChunkRefs();
                item->SetVChunkIndex(vChunkIndex);
                item->SetChunkIdx(chunkIdx);
                fillExtentRef(item, tabletId, vChunkIndex);
            }
        }

        for (const auto& entry : CommittedIntegrityChunks) {
            auto *chunk = snapshot->AddIntegrityChunks();
            chunk->SetChunkIdx(entry.ChunkIdx);
            chunk->SetGeneration(entry.Generation);
        }
        snapshot->SetGenerationCounter(IntegrityManager->GetGenerationCounter());

        ++*Counters.RecoveryLog.NumChunkMapSnapshots;
        return record;
    }

    NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord TDDiskActor::CreateChunkMapIncrement(ui64 tabletId,
            ui64 vChunkIndex, TChunkIdx chunkIdx, const TIntegrityManager::TExtentRef& extentRef,
            const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk) {
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
        auto *increment = record.MutableIncrement();
        if (integrityChunk) {
            auto *chunk = increment->MutableIntegrityChunk();
            chunk->SetChunkIdx(integrityChunk->ChunkIdx);
            chunk->SetGeneration(integrityChunk->Generation);
        }

        auto *data = increment->MutableDataChunk();
        data->SetTabletId(tabletId);
        data->SetVChunkIndex(vChunkIndex);
        data->SetChunkIdx(chunkIdx);

        auto *ref = data->MutableExtentRef();
        ref->SetIntegrityChunkIdx(extentRef.IntegrityChunkIdx);
        ref->SetExtentSlot(extentRef.ExtentSlot);
        ref->SetVChunkGeneration(extentRef.VChunkGeneration);

        ++*Counters.RecoveryLog.NumChunkMapIncrements;
        return record;
    }

    void TDDiskActor::Handle(TEvDeleteTabletChunks::TPtr ev) {
        if (!CheckQuery(*ev, nullptr)) {
            return;
        }

        const TQueryCredentials creds(ev->Get()->Record.GetCredentials());
        const ui64 tabletId = creds.TabletId;

        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvDeleteTabletChunks)",
            {"marker", "BSDD51"},
            {"DDiskId", DDiskId},
            {"tabletId", tabletId});

        if (TabletChunkDeletionsInFlight.contains(tabletId)) {
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                "tablet chunk deletion is in flight"));
            return;
        }

        // Source reads and target writes of an in-flight sync may not have reached the target
        // chunk yet. Deleting now could free the physical chunk underneath a write or let a late
        // source result recreate the just-deleted mapping.
        for (const auto& [syncId, sync] : SyncsInFlight) {
            Y_UNUSED(syncId);
            if (sync.Creds.TabletId == tabletId) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "sync is in flight for tablet"));
                return;
            }
        }

        // Reject if any chunk allocation for this tablet is in flight (covers both allocations
        // whose increment log record is pending and those still waiting for an extent ref).
        for (const auto& [key, allocation] : DataChunkAllocationsInFlight) {
            Y_UNUSED(allocation);
            if (key.first == tabletId) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "chunk allocation is in flight for tablet"));
                return;
            }
        }

        if (IntegrityManager->HasInFlightOperationsForTablet(tabletId)) {
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                "integrity I/O is in flight for tablet"));
            return;
        }

        const auto tabletIt = ChunkRefs.find(tabletId);

        if (tabletIt == ChunkRefs.end()) {
            // tablet has no chunks
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            return;
        }

        // Reject if any VChunk has a pending event queue (allocation queued but not yet in log)
        // or client data I/O that still targets its physical chunk.
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second) {
            if (!chunkRef.PendingEventsForChunk.empty()
                    || !chunkRef.PendingSerializedWrites.empty()
                    || chunkRef.IntegrityExtentWriteInFlight) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "chunk allocation or integrity-extent write is queued for tablet"));
                return;
            }
            if (chunkRef.InFlightDataIo) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "data chunk I/O is in flight for tablet"));
                return;
            }
        }

        // Collect physical data chunk IDs.
        TVector<TChunkIdx> chunksToDelete;
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second) {
            if (chunkRef.ChunkIdx) {
                chunksToDelete.push_back(chunkRef.ChunkIdx);
            }
        }

        if (chunksToDelete.empty()) {
            ChunkRefs.erase(tabletIt);
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            return;
        }

        // Remove the logical mapping from the snapshot now, but quarantine the corresponding
        // integrity slots until this removal record commits. Formatting a reused slot earlier
        // could overwrite metadata that recovery still maps to this tablet after a crash.
        const bool inserted = TabletChunkDeletionsInFlight.insert(tabletId).second;
        Y_ABORT_UNLESS(inserted);
        IntegrityManager->PrepareTabletChunksDeletion(tabletId);
        ChunkRefs.erase(tabletIt);

        *Counters.Chunks.ChunksOwned -= chunksToDelete.size();

        // Capture reply info before issuing the async log record
        const TActorId replyTo = ev->Sender;
        const ui64 replyCookie = ev->Cookie;
        const TActorId replySession = ev->InterconnectSession;
        const bool replyInserted = TabletChunkDeletionReplies.emplace(tabletId,
            TTabletChunkDeletionReply{
                .ReplyTo = replyTo,
                .Cookie = replyCookie,
                .InterconnectSession = replySession,
            }).second;
        Y_ABORT_UNLESS(replyInserted);

        // The first snapshot removes and deallocates only the data chunks. Integrity chunks stay
        // owned because their deleted extents are not reusable until this snapshot is durable.
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(),
            &ChunkMapSnapshotLsn,
            [this, tabletId]() {
                const size_t erased = TabletChunkDeletionsInFlight.erase(tabletId);
                Y_ABORT_UNLESS(erased == 1);
                IntegrityManager->CommitTabletChunksDeletion(tabletId);

                // Freed slots serve pending allocations first. Any integrity chunks left empty are
                // removed by a second snapshot; acknowledge deletion only after that record lands.
                ReclaimUnusedIntegrityChunks([this, tabletId]() {
                    const auto replyIt = TabletChunkDeletionReplies.find(tabletId);
                    Y_ABORT_UNLESS(replyIt != TabletChunkDeletionReplies.end());
                    const auto& reply = replyIt->second;
                    auto h = std::make_unique<IEventHandle>(reply.ReplyTo, SelfId(),
                        new TEvDeleteTabletChunksResult(NKikimrBlobStorage::NDDisk::TReplyStatus::OK),
                        0, reply.Cookie);
                    if (reply.InterconnectSession) {
                        h->Rewrite(TEvInterconnect::EvForward, reply.InterconnectSession);
                    }
                    TActivationContext::Send(h.release());
                    TabletChunkDeletionReplies.erase(replyIt);
                });
            },
            std::move(chunksToDelete));
    }

} // NKikimr::NDDisk
