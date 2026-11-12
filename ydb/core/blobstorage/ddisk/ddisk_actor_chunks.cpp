#include "ddisk_actor.h"
#include "direct_io_op.h"
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>

#include <algorithm>
#include <util/generic/scope.h>
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/interconnect.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    void TDDiskActor::SubmitIntegrityWork(TIntegrityManager::TWork work) {
        TDataRequestGuard guard(*this);
        const bool allocationsChanged = !work.ReturnedChunks.empty() || !work.Allocations.empty();
        for (auto chunk : work.ReturnedChunks) {
            ChunkManager.ReturnChunk(chunk);
        }
        for (auto token : work.Allocations) {
            if (Stopping || IsBroken()) {
                SubmitIntegrityWork(IntegrityManager->CompleteAllocation(token, 0));
            } else {
                ChunkManager.Enqueue(TChunkForIntegrity{token});
            }
        }
        for (auto& write : work.Writes) {
            SubmitIntegrityWrite(std::move(write));
        }
        IntegrityManager->NotifyCompleted();
        if (allocationsChanged && !Stopping) {
            HandleChunkReserved();
        }
    }

    void TDDiskActor::SubmitIntegrityWrite(TIntegrityManager::TWriteSubmission write) {
        TDataRequestGuard requestGuard(*this);
        auto batch = AllocateBatchedIOAwaiter();
        bool ok = false;
        if (!Stopping && !IsBroken()) {
            batch->Add(PrepareCriticalWrite(write.ChunkIdx, write.OffsetInBytes, std::move(write.Data)));
            co_await batch->Wait();
            ok = batch->DataResult.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            if (!ok && !Stopping) {
                EnterBroken(batch->DataResult.ErrorMessage);
            }
        }
        SubmitIntegrityWork(IntegrityManager->CompleteWrite(write.Id, ok && !IsBroken()));
        ReturnBatchedIOAwaiter(std::move(batch));
        if (!Stopping && !IsBroken()) {
            PrepareIntegrityReclamation();
        }
        requestGuard.Release();
        if (Stopping && !GetDirectIoInflight()) {
            FinishStopping();
        }
    }

    std::unique_ptr<TDDiskActor::TDDiskIoOp> TDDiskActor::PrepareCriticalWrite(TChunkIdx chunk, ui32 offset,
            TRcBuf data, std::optional<size_t> metadataIndex)
    {
        auto write = AllocateOp<TDDiskIoOp>();
        write->SetCritical();
        if (metadataIndex) {
            write->SetMetadataIndex(*metadataIndex);
        }
        write->PrepareWrite(TRope(std::move(data)), DiskFormat->Offset(chunk, 0, offset), chunk, offset);
        return write;
    }

    void TDDiskActor::CountIntegrityResult(const TIntegrityManager::TOperationResult& result) {
        if (result.Status == TIntegrityManager::EOperationStatus::Corrupted) {
            Counters.Checksums.IntegrityCorruption->Inc();
            if (result.LostWriteDetected) {
                Counters.Checksums.IntegrityLostWriteDetected->Inc();
            }
        }
    }

    bool TDDiskActor::IsChunkCommitted(ui64 tabletId, ui64 vChunkIndex) const {
        return !IsBroken() && !DataChunkAllocationsInFlight.contains({tabletId, vChunkIndex});
    }

    void TDDiskActor::IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex) {
        if (Stopping || Y_UNLIKELY(IsBroken())) {
            return;
        }
        Tablets.at(tabletId).ChunkRefs.at(vChunkIndex).AllocationPending = true;
        const ui64 token = NextDataAllocationToken++;
        Y_ABORT_UNLESS(DataChunkAllocationsInFlight.emplace(std::make_pair(tabletId, vChunkIndex),
            TDataChunkAllocationInFlight{.Token = token}).second);
        ChunkManager.Enqueue(TChunkForData{tabletId, vChunkIndex, token});
        HandleChunkReserved();
    }

    void TDDiskActor::Handle(TEvPrivate::TEvIssuePersistentBufferChunkAllocation::TPtr ev) {
        if (!CanHandleQuery(ev)) {
            return;
        }
        if (!IssuePersistentBufferChunkAllocationInflight) {
            IssuePersistentBufferChunkAllocationInflight = true;
            ChunkManager.Enqueue(TChunkForPersistentBuffer{});
            HandleChunkReserved();
        }
    }

    void TDDiskActor::Handle(TEvPrivate::TEvDeallocatePersistentBufferChunk::TPtr ev) {
        auto chunkIdx = ev->Get()->ChunkIdx;
        auto it = std::find(PersistentBufferChunks.begin(), PersistentBufferChunks.end(), chunkIdx);
        Y_DEBUG_ABORT_UNLESS(it != PersistentBufferChunks.end());
        PersistentBufferChunks.erase(it);
        auto ticket = std::make_shared<TLogTicket>();
        IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, 0,
            CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn,
            {chunkIdx}, ticket);
        if (co_await WaitForLog(ticket)) {
            Send(PersistentBufferActorId, new TEvPrivate::TEvDeallocatePersistentBufferChunkResult(chunkIdx));
            --*Counters.Chunks.ChunksOwned;
        }
    }

    void TDDiskActor::ReserveChunks(size_t count) {
        YDB_LOG_DEBUG("TDDiskActor::ReserveChunks requesting chunk reserve",
            {"marker", "BSDD28"},
            {"DDiskId", DDiskId},
            {"chunkReserveSize", ChunkManager.GetReservedChunkCount()},
            {"minChunksReserved", MinChunksReserved},
            {"formattingChunks", FormattingChunks.size()},
            {"requestCount", count});
        ChunkManager.BeginReservation();
        const ui64 cookie = NActors::AllocateWaitCookie();
        Y_ABORT_UNLESS(!ReservationCookie);
        ReservationCookie = cookie;
        auto request = std::make_unique<NPDisk::TEvChunkReserve>(PDiskParams->Owner,
            PDiskParams->OwnerRound, count);
        request->IsDDisk = true;
        Send(BaseInfo.PDiskActorID, request.release(), IEventHandle::FlagTrackDelivery, cookie);
    }

    void TDDiskActor::Handle(NPDisk::TEvChunkReserveResult::TPtr ev) {
        if (!ReservationCookie || *ReservationCookie != ev->Cookie) {
            return;
        }
        ReservationCookie.reset();
        ChunkManager.FinishReservation();
        const auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::ReserveChunks received reserve result",
            {"marker", "BSDD04"},
            {"DDiskId", DDiskId},
            {"msg", msg});
        if (Stopping) {
            if (msg.Status == NKikimrProto::OK) {
                for (TChunkIdx chunk : msg.ChunkIds) {
                    ChunkManager.ReturnChunk(chunk);
                }
                if (OwnDrainFinishing) {
                    ReleaseUncommittedChunks();
                }
            }
            TryCompleteStop();
            return;
        }
        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "ReserveChunks")) {
            return;
        }
        for (TChunkIdx chunk : msg.ChunkIds) {
            if (Config.EnableChecksums || IsBroken()) {
                ChunkManager.ReturnChunk(chunk);
            } else {
                Y_ABORT_UNLESS(FormattingChunks.insert(chunk).second);
                FormatChunk(chunk);
            }
        }
        HandleChunkReserved();
    }

    void TDDiskActor::ReleaseUncommittedChunks() {
        if (IsPersistentBufferActor || !PDiskParams || !LogReplayComplete) {
            return;
        }
        Y_ABORT_UNLESS(Stopping && !GetDirectIoInflight() && !DataRequestsInFlight);

        TVector<TChunkIdx> chunks = ChunkManager.ExtractReservations();
        for (const auto chunkIdx : FormattingChunks) {
            chunks.push_back(chunkIdx);
        }
        FormattingChunks.clear();
        chunks.insert(chunks.end(), PendingChunkRelease.begin(), PendingChunkRelease.end());
        PendingChunkRelease.clear();
        for (const auto& [_, allocation] : DataChunkAllocationsInFlight) {
            // A submitted commit can still succeed after this actor stops.
            if (allocation.ChunkIdx && !allocation.LogIssued) {
                chunks.push_back(allocation.ChunkIdx);
            }
        }
        if (IntegrityManager) {
            for (const TChunkIdx chunkIdx : IntegrityManager->GetIntegrityChunkIdxs()) {
                if (!IsIntegrityChunkCommitted(chunkIdx)) {
                    chunks.push_back(chunkIdx);
                }
            }
        }
        std::sort(chunks.begin(), chunks.end());
        chunks.erase(std::unique(chunks.begin(), chunks.end()), chunks.end());
        // PDisk validates the entire batch: repeating an already forgotten ID
        // would reject fresh reservations in the same request as well.
        std::erase_if(chunks, [this](TChunkIdx chunkIdx) {
            return !ShutdownChunkReleasesIssued.insert(chunkIdx).second;
        });
        if (!chunks.empty()) {
            YDB_LOG_NOTICE("DDisk releasing uncommitted reservations", {"DDiskId", DDiskId}, {"chunks", chunks});
            auto request = std::make_unique<NPDisk::TEvChunkForget>(
                PDiskParams->Owner, PDiskParams->OwnerRound, std::move(chunks));
            request->IsDDisk = true;
            Send(BaseInfo.PDiskActorID, request.release());
        }
    }

    void TDDiskActor::FormatChunk(TChunkIdx chunkIdx) {
        static constexpr ui32 FormatSliceSize = 16u << 20;
        Y_ABORT_UNLESS(!Config.EnableChecksums && DiskFormat->ChunkSize <= Max<ui32>());
        TDataRequestGuard requestGuard(*this);
        auto batch = AllocateBatchedIOAwaiter();
        ui32 offset = 0;
        while (offset < DiskFormat->ChunkSize && !Stopping && !IsBroken()) {
            const ui32 size = Min(FormatSliceSize, static_cast<ui32>(DiskFormat->ChunkSize) - offset);
            auto zero = TRcBuf::UninitializedPageAligned(size);
            memset(zero.GetDataMut(), 0, size);
            batch->Add(PrepareCriticalWrite(chunkIdx, offset, std::move(zero)));
            co_await batch->Wait();
            if (batch->DataResult.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                if (!Stopping && !IsBroken()) {
                    EnterBroken(TStringBuilder() << "failed to zero-format newly reserved chunk " << chunkIdx
                        << " at offset " << offset << ": " << batch->DataResult.ErrorMessage);
                }
                break;
            }
            offset += size;
            batch->ClearForReuse();
        }
        FormattingChunks.erase(chunkIdx);
        if (Stopping || IsBroken() || offset != DiskFormat->ChunkSize) {
            PendingChunkRelease.insert(chunkIdx);
        } else {
            ChunkManager.ReturnChunk(chunkIdx);
            HandleChunkReserved();
        }
        ReturnBatchedIOAwaiter(std::move(batch));
        requestGuard.Release();
        if (Stopping && !GetDirectIoInflight()) {
            FinishStopping();
        }
    }

    void TDDiskActor::HandleChunkReserved() {
        if (Stopping) {
            return;
        }
        if (HandlingChunkReserved) {
            ChunkReservedAgain = true;
            return;
        }
        HandlingChunkReserved = true;
        Y_DEFER { HandlingChunkReserved = false; };
        Y_ABORT_UNLESS(!IsPersistentBufferActor);
        if (IsBroken()) {
            // Broken retains PB service but must never hand a physical chunk to a canceled
            // data/integrity request while manager Stop is draining its callbacks.
            ChunkManager.RetainPersistentBufferAllocations();
        }
        do {
            ChunkReservedAgain = false;
            while (auto allocation = ChunkManager.TakeAllocation()) {
                const auto& [chunkAllocate, chunkIdx] = *allocation;
                AllocateChunk(chunkAllocate, chunkIdx);
                // Chunk-map increments (data and integrity alike) need a snapshot starting point to
                // replay from.
                if (!std::holds_alternative<TChunkForPersistentBuffer>(chunkAllocate)
                        && ChunkMapSnapshotLsn == Max<ui64>()) {
                    IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(),
                        &ChunkMapSnapshotLsn);
                }
            }
            const size_t count = IsBroken()
                ? ChunkManager.GetRefillCount(ChunkManager.CountPendingPersistentBufferAllocations())
                : ChunkManager.GetRefillCount(MinChunksReserved, FormattingChunks.size());
            if (count) {
                ReserveChunks(count);
            }
        }
        while (ChunkReservedAgain && !Stopping);
    }

    void TDDiskActor::AllocateChunk(TChunkManager::TAllocation allocation, TChunkIdx chunkIdx) {
        if (const auto* data = std::get_if<TChunkForData>(&allocation)) {
            AllocateDataChunk(data->TabletId, data->VChunkIndex, data->Token, chunkIdx);
        } else if (const auto* integrity = std::get_if<TChunkForIntegrity>(&allocation)) {
            SubmitIntegrityWork(IntegrityManager->CompleteAllocation(integrity->Token, chunkIdx));
        } else {
            AllocatePersistentBufferChunk(chunkIdx);
        }
    }

    void TDDiskActor::AllocatePersistentBufferChunk(TChunkIdx chunkIdx) {
        Y_DEBUG_ABORT_UNLESS(std::find(PersistentBufferChunks.begin(),
            PersistentBufferChunks.end(), chunkIdx) == PersistentBufferChunks.end());
        PersistentBufferChunks.emplace_back(chunkIdx);
        auto ticket = std::make_shared<TLogTicket>();
        IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap,
            chunkIdx, CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn,
            {}, ticket);
        if (co_await WaitForLog(ticket)) {
            IssuePersistentBufferChunkAllocationInflight = false;
            Send(PersistentBufferActorId, new TEvPrivate::TEvHandlePersistentBufferEventForChunk(chunkIdx));
            ++*Counters.Chunks.ChunksOwned;
        }
    }

    void TDDiskActor::AllocateDataChunk(ui64 tabletId, ui64 vChunkIndex, ui64 token, TChunkIdx chunkIdx) {
        const auto key = std::make_pair(tabletId, vChunkIndex);
        auto current = [&] {
            const auto it = DataChunkAllocationsInFlight.find(key);
            return it != DataChunkAllocationsInFlight.end() && it->second.Token == token
                && !Stopping && !IsBroken();
        };
        if (!current()) {
            ChunkManager.ReturnChunk(chunkIdx);
            co_return;
        }
        DataChunkAllocationsInFlight.at(key).ChunkIdx = chunkIdx;
        TDataRequestGuard requestGuard(*this);
        {
            auto& chunk = Tablets.at(tabletId).ChunkRefs.at(vChunkIndex);
            ++chunk.ChunkRefPins;
            Y_DEFER { --chunk.ChunkRefPins; };
            std::optional<TIntegrityManager::TExtent> extent;
            do {
                if (Config.EnableChecksums) {
                    auto started = IntegrityManager->StartExtent({tabletId, vChunkIndex}, chunkIdx);
                    extent.emplace(std::move(started.first));
                    SubmitIntegrityWork(std::move(started.second));
                    while (current() && !extent->GetPlacedResult()) {
                        auto waiter = extent->WaitChanged();
                        co_await NonCancellable(waiter);
                    }
                    if (!current() || !extent->GetPlacedResult().value_or(false)) {
                        break;
                    }
                }
                SetDataChunkMapping(tabletId, &chunk, chunkIdx);
                chunk.AllocationPending = false;
                chunk.AllocationReady.NotifyAll();
                if (extent) {
                    while (current() && !extent->GetReadyResult()) {
                        auto waiter = extent->WaitChanged();
                        co_await NonCancellable(waiter);
                    }
                    if (!current() || !extent->GetReadyResult().value_or(false)) {
                        break;
                    }
                }
                if (current() && (co_await WaitForLog(CommitDataChunk(tabletId, vChunkIndex, token)))
                        && current()) {
                    CompleteDataChunkAllocation(tabletId, vChunkIndex, token);
                }
            } while (false);
        }
        requestGuard.Release();
        if (Stopping && !GetDirectIoInflight()) {
            FinishStopping();
        }
    }

    bool TDDiskActor::IsIntegrityChunkCommitted(TChunkIdx chunkIdx) const {
        return std::any_of(CommittedIntegrityChunks.begin(), CommittedIntegrityChunks.end(),
            [chunkIdx](const auto& entry) { return entry.ChunkIdx == chunkIdx; });
    }

    TDDiskActor::TIntegrityReclamation TDDiskActor::PrepareIntegrityReclamation(bool wait) {
        if (Stopping) {
            return {false, {}};
        }
        if (!Config.EnableChecksums) {
            return {true, {}};
        }
        Y_ABORT_UNLESS(Config.EnableChecksums && IntegrityManager);
        if (Y_UNLIKELY(IsBroken())) {
            return {false, {}};
        }

        auto work = IntegrityManager->TakeReleasableIntegrityChunks();
        const auto releasableChunks = std::exchange(work.ReturnedChunks, {});
        SubmitIntegrityWork(std::move(work));

        TVector<TChunkIdx> chunksToDelete;
        for (const TChunkIdx chunkIdx : releasableChunks) {
            if (IsIntegrityChunkCommitted(chunkIdx)) {
                const size_t erased = std::erase_if(CommittedIntegrityChunks, [chunkIdx](const auto& entry) {
                    return entry.ChunkIdx == chunkIdx;
                });
                Y_ABORT_UNLESS(erased == 1);
                chunksToDelete.push_back(chunkIdx);
            } else {
                ChunkManager.ReturnChunk(chunkIdx);
            }
        }

        if (chunksToDelete.empty()) {
            return {true, {}};
        }

        *Counters.Chunks.ChunksOwned -= chunksToDelete.size();
        auto ticket = wait ? std::make_shared<TLogTicket>() : nullptr;
        if (ticket) {
            ticket->IsDDisk = true;
        }
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, TChunkIdx(0), CreateChunkMapSnapshot(),
            &ChunkMapSnapshotLsn, std::move(chunksToDelete), ticket);
        return {true, std::move(ticket)};
    }

    std::shared_ptr<TDDiskActor::TLogTicket> TDDiskActor::CommitDataChunk(ui64 tabletId, ui64 vChunkIndex, ui64 token) {
        if (Stopping || Y_UNLIKELY(IsBroken())) {
            return {};
        }

        const auto it = DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
        if (it == DataChunkAllocationsInFlight.end() || it->second.Token != token) {
            return {};
        }
        auto& allocation = it->second;
        if (allocation.LogIssued) {
            return {};
        }

        allocation.LogIssued = true;
        const TChunkIdx chunkIdx = allocation.ChunkIdx;

        TVector<TChunkIdx> commitChunks;
        const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk = nullptr;
        TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry integrityEntry;
        const TIntegrityManager::TExtentRef* ref = nullptr;
        if (Config.EnableChecksums) {
            Y_ABORT_UNLESS(IntegrityManager);
            Y_ABORT_UNLESS(IntegrityManager->IsExtentReady({tabletId, vChunkIndex}));
            ref = IntegrityManager->FindExtentRef({tabletId, vChunkIndex});
            Y_ABORT_UNLESS(ref);
            if (!IsIntegrityChunkCommitted(ref->IntegrityChunkIdx)) {
                integrityEntry = {
                    .ChunkIdx = ref->IntegrityChunkIdx,
                    .Generation = IntegrityManager->GetIntegrityChunkGeneration(ref->IntegrityChunkIdx),
                };
                CommittedIntegrityChunks.push_back(integrityEntry);
                integrityChunk = &CommittedIntegrityChunks.back();
                commitChunks.push_back(ref->IntegrityChunkIdx);
            }
        }
        commitChunks.push_back(chunkIdx);
        allocation.NewlyCommittedChunks = commitChunks.size();

        auto ticket = std::make_shared<TLogTicket>();
        ticket->IsDDisk = true;
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, std::move(commitChunks),
            CreateChunkMapIncrement(tabletId, vChunkIndex, chunkIdx, ref, integrityChunk),
            nullptr, {}, ticket);
        return ticket;
    }

    void TDDiskActor::CompleteDataChunkAllocation(ui64 tabletId, ui64 vChunkIndex, ui64 token) {
        if (Y_UNLIKELY(IsBroken())) {
            return;
        }

        const auto it = DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
        if (it == DataChunkAllocationsInFlight.end() || it->second.Token != token) {
            return;
        }
        auto allocation = std::move(it->second);
        DataChunkAllocationsInFlight.erase(it);

        TChunkRef& chunkRef = Tablets[tabletId].ChunkRefs[vChunkIndex];
        Y_ABORT_UNLESS(chunkRef.ChunkIdx == allocation.ChunkIdx);
        Y_ABORT_UNLESS(chunkRef.ChunkRefPins);

        Y_ABORT_UNLESS(allocation.LogIssued);
        *Counters.Chunks.ChunksOwned += allocation.NewlyCommittedChunks;

        chunkRef.CommitReady.NotifyAll();
        QueueSyncsForChunk(tabletId, vChunkIndex);
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
            IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0, CreateChunkMapSnapshot(), &ChunkMapSnapshotLsn);
        }
        if (PersistentBufferChunkMapSnapshotLsn < freeUpToLsn) { // we have to rewrite snapshot
            IssuePDiskLogRecord(TLogSignature::SignaturePersistentBufferChunkMap, 0, CreatePersistentBufferChunkMapSnapshot(), &PersistentBufferChunkMapSnapshotLsn);
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
        record.SetChecksumsDisabled(!Config.EnableChecksums);
        auto *snapshot = record.MutableSnapshot();

        const auto fillExtentRef = [this](auto *item, ui64 tabletId, ui64 vChunkIndex) {
            Y_ABORT_UNLESS(Config.EnableChecksums && IntegrityManager);
            // Non-null for every chunk with a log record: the extent is Ready by the time its
            // increment is issued, and refs survive until the chunk is deleted.
            const auto *ref = IntegrityManager->FindExtentRef({tabletId, vChunkIndex});
            Y_ABORT_UNLESS(ref);
            auto *extentRef = item->MutableExtentRef();
            extentRef->SetIntegrityChunkIdx(ref->IntegrityChunkIdx);
            extentRef->SetExtentSlot(ref->ExtentSlot);
            extentRef->SetVChunkGeneration(ref->VChunkGeneration);
        };

        for (const auto& [tabletId, tablet] : Tablets) {
            const auto& chunks = tablet.ChunkRefs;
            if (chunks.empty()) {
                continue;
            }
            auto *tabletRecord = snapshot->AddTabletRecords();
            tabletRecord->SetTabletId(tabletId);

            for (const auto& [vChunkIndex, chunkRef] : chunks) {
                if (!chunkRef.ChunkIdx) {
                    continue;
                }
                if (const auto allocation = DataChunkAllocationsInFlight.find({tabletId, vChunkIndex});
                        allocation != DataChunkAllocationsInFlight.end() && !allocation->second.LogIssued) {
                    // Include issued increments: PDisk commits them before this snapshot.
                    continue;
                }
                auto *item = tabletRecord->AddChunkRefs();
                item->SetVChunkIndex(vChunkIndex);
                item->SetChunkIdx(chunkRef.ChunkIdx);
                if (Config.EnableChecksums) {
                    fillExtentRef(item, tabletId, vChunkIndex);
                }
            }
        }

        if (Config.EnableChecksums) {
            for (const auto& entry : CommittedIntegrityChunks) {
                auto *chunk = snapshot->AddIntegrityChunks();
                chunk->SetChunkIdx(entry.ChunkIdx);
                chunk->SetGeneration(entry.Generation);
            }
            snapshot->SetGenerationCounter(IntegrityManager->GetGenerationCounter());
        }

        ++*Counters.RecoveryLog.NumChunkMapSnapshots;
        return record;
    }

    NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord TDDiskActor::CreateChunkMapIncrement(ui64 tabletId,
            ui64 vChunkIndex, TChunkIdx chunkIdx, const TIntegrityManager::TExtentRef* extentRef,
            const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk) {
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord record;
        record.SetChecksumsDisabled(!Config.EnableChecksums);
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

        if (extentRef) {
            auto *ref = data->MutableExtentRef();
            ref->SetIntegrityChunkIdx(extentRef->IntegrityChunkIdx);
            ref->SetExtentSlot(extentRef->ExtentSlot);
            ref->SetVChunkGeneration(extentRef->VChunkGeneration);
        }

        ++*Counters.RecoveryLog.NumChunkMapIncrements;
        return record;
    }

    void TDDiskActor::Handle(TEvDeleteTabletChunks::TPtr ev) {
        if (!CheckQuery(*ev, nullptr)) {
            co_return;
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
            co_return;
        }

        // Source reads and target writes of an in-flight sync may not have reached the target
        // chunk yet. Deleting now could free the physical chunk underneath a write or let a late
        // source result recreate the just-deleted mapping.
        for (const auto& [syncId, sync] : SyncsInFlight) {
            Y_UNUSED(syncId);
            if (sync->Creds.TabletId == tabletId) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "sync is in flight for tablet"));
                co_return;
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
                co_return;
            }
        }

        if (Config.EnableChecksums && IntegrityManager->HasInFlightOperationsForTablet(tabletId)) {
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                "integrity I/O is in flight for tablet"));
            co_return;
        }

        const auto tabletIt = Tablets.find(tabletId);

        if (tabletIt == Tablets.end()) {
            // tablet has no chunks
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            co_return;
        }

        // ChunkRefPins protect the mapping and physical chunk throughout requests,
        // including allocation, data I/O, metadata, and commit waits.
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second.ChunkRefs) {
            if (chunkRef.AllocationPending || chunkRef.ChunkRefPins) {
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                    "chunk allocation or integrity-extent write is queued for tablet"));
                co_return;
            }
        }

        // Collect physical data chunk IDs.
        TVector<TChunkIdx> chunksToDelete;
        for (const auto& [vChunkIndex, chunkRef] : tabletIt->second.ChunkRefs) {
            if (chunkRef.ChunkIdx) {
                chunksToDelete.push_back(chunkRef.ChunkIdx);
            }
        }

        if (chunksToDelete.empty()) {
            tabletIt->second.ChunkRefs.clear();
            CountTabletChunks(tabletId, 0);
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            co_return;
        }

        // Remove the logical mapping from the snapshot now, but quarantine the corresponding
        // integrity slots until this removal record commits. Formatting a reused slot earlier
        // could overwrite metadata that recovery still maps to this tablet after a crash.
        const bool inserted = TabletChunkDeletionsInFlight.insert(tabletId).second;
        Y_ABORT_UNLESS(inserted);
        if (Config.EnableChecksums) {
            IntegrityManager->PrepareTabletChunksDeletion(tabletId);
        }
        CountTabletChunks(tabletId, -static_cast<i64>(chunksToDelete.size()));
        tabletIt->second.ChunkRefs.clear();

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
        auto ticket = std::make_shared<TLogTicket>();
        ticket->IsDDisk = true;
        IssuePDiskLogRecord(TLogSignature::SignatureDDiskChunkMap, 0,
            CreateChunkMapSnapshot(), &ChunkMapSnapshotLsn, std::move(chunksToDelete),
            ticket);
        if (!(co_await WaitForLog(ticket))) {
            co_return;
        }
        TabletChunkDeletionsInFlight.erase(tabletId);
        if (Config.EnableChecksums) {
            IntegrityManager->CommitTabletChunksDeletion(tabletId);
            IntegrityManager->NotifyCompleted();
            const auto reclamation = PrepareIntegrityReclamation(true);
            if (!reclamation.Ok || (reclamation.Ticket && !(co_await WaitForLog(reclamation.Ticket)))) {
                co_return;
            }
        }
        // Broken/Stopping consumes the registry and sends the terminal reply.
        if (TabletChunkDeletionReplies.erase(tabletId)) {
            // Session replacement cannot undo this already durable deletion.
            SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
        }
    }

} // NKikimr::NDDisk
