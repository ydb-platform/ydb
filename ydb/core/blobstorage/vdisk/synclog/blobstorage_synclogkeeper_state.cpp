#include "blobstorage_synclogkeeper_state.h"

#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>

using namespace NKikimrServices;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // Utilities
        ////////////////////////////////////////////////////////////////////////////
        namespace {
            static ui32 CalcMaxMemPages(ui64 maxMemAmount, ui32 appendBlockSize) {
                ui32 maxMemPages = maxMemAmount / appendBlockSize;
                return (maxMemPages < 2) ? 2 : maxMemPages;
            }

            static ui32 CalcMaxDiskChunks(ui64 maxDiskAmount, ui32 chunkSize) {
                ui32 maxDiskChunks = maxDiskAmount / chunkSize;
                return (maxDiskChunks < 2) ? 2 : maxDiskChunks;
            }

        } // namespace

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogKeeperState
        ////////////////////////////////////////////////////////////////////////////
        TSyncLogKeeperState::TSyncLogKeeperState(
                TIntrusivePtr<TVDiskContext> vctx,
                std::unique_ptr<TSyncLogRepaired> repaired,
                ui64 syncLogMaxMemAmount,
                ui64 syncLogMaxDiskAmount,
                ui64 syncLogMaxEntryPointSize)
            : VCtx(std::move(vctx))
            , SyncLogPtr(std::move(repaired->SyncLogPtr))
            , ChunksToDelete(std::move(repaired->ChunksToDelete))
            , LastCommit(repaired->CommitHistory)
            , MaxMemPages(CalcMaxMemPages(syncLogMaxMemAmount, SyncLogPtr->GetAppendBlockSize()))
            , MaxDiskChunks(CalcMaxDiskChunks(syncLogMaxDiskAmount, SyncLogPtr->GetChunkSize()))
            , SyncLogMaxEntryPointSize(syncLogMaxEntryPointSize)
            , NeedsInitialCommit(repaired->NeedsInitialCommit)
        {
            if (!ChunksToDelete.empty()) {
                DelayedActions.SetDeleteChunk();
            }
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages) {
                DelayedActions.SetMemOverflow();
            }
        }

        // Calculate first lsn in recovery log we must to keep
        ui64 TSyncLogKeeperState::CalculateFirstLsnToKeep() const {
            // calculate first lsn for data
            ui64 firstDataInRecovLogLsnToKeep = CalculateFirstDataInRecovLogLsnToKeep();
            // we still want to keep data in recovery log
            ui64 result = Min(firstDataInRecovLogLsnToKeep, LastCommit.FirstLsnToKeep());
            return result;
        }

        TString TSyncLogKeeperState::CalculateFirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{FreeUpToLsn# " << FreeUpToLsn
                << " LastCommit.EntryPointLsn# " << LastCommit.EntryPointLsn
                << " LastCommit.RecoveryLogConfirmedLsn# " << LastCommit.RecoveryLogConfirmedLsn
                << " TrimTailLsn# " << TrimTailLsn
                << " firstDataInRecovLogLsnToKeep# " << SyncLogPtr->FirstLsnToKeep()
                << " firstDataInRecovLogLsnToKeepDecomposed# " << SyncLogPtr->FirstLsnToKeepDecomposed()
                << "}";
            return str.Str();
        }

        void TSyncLogKeeperState::PutOne(const TRecordHdr *rec, ui32 size) {
            SyncLogPtr->PutOne(rec, size);
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.SetMemOverflow();
        }

        void TSyncLogKeeperState::PutMany(const void *buf, ui32 size) {
            SyncLogPtr->PutMany(buf, size);
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.SetMemOverflow();
        }

        // put the whole level into SyncLog
        void TSyncLogKeeperState::PutLevelSegment(const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> *seg) {
            char buffer[MaxRecFullSize];
            TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TMemIterator it(seg);
            ui64 lsn = seg->Info.FirstLsn;
            it.SeekToFirst();
            while (it.Valid()) {
                ui32 len = TSerializeRoutines::SetLogoBlob(VCtx->Top->GType,
                        buffer,
                        lsn,
                        it->Key.LogoBlobID(),
                        it->MemRec.GetIngress());
                Y_DEBUG_ABORT_UNLESS(len <= sizeof(buffer));
                SyncLogPtr->PutOne(reinterpret_cast<const TRecordHdr *>(buffer), len);
                it.Next();
                ++lsn;
            }
            Y_ABORT_UNLESS(lsn <= seg->Info.LastLsn + 1);
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.SetMemOverflow();
        }

        void TSyncLogKeeperState::TrimTailEvent(ui64 trimTailLsn) {
            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: TEvSyncLogTrim: trimLsn# %" PRIu64, trimTailLsn));

            TrimTailLsn = trimTailLsn;
            DelayedActions.SetTrimTail();
        }

        void TSyncLogKeeperState::BaldLogEvent() {
            const ui64 baldLsn = SyncLogPtr->GetLastLsn();
            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: TEvSyncLogBaldLog: baldLsn# %" PRIu64, baldLsn));

            TrimTailLsn = baldLsn;
            DelayedActions.SetTrimTail();
        }

        void TSyncLogKeeperState::CutLogEvent(ui64 freeUpToLsn) {
            LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: NPDisk::TEvCutLog: freeUpToLsn# %" PRIu64, freeUpToLsn));

            FreeUpToLsn = freeUpToLsn;
            CutLogRetries = 0;
            LastCutLogRetryFirstLsnToKeep = CalculateFirstLsnToKeep();
            DelayedActions.SetCutLog();
        }

        void TSyncLogKeeperState::RetryCutLogEvent() {
            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            if (firstLsnToKeep > LastCutLogRetryFirstLsnToKeep) {
                CutLogRetries = 0;
                LastCutLogRetryFirstLsnToKeep = firstLsnToKeep;
            } else if (++CutLogRetries > 2) {
                // error condition, retry doesn't help if several commits did not advance the boundary
                LOG_ERROR(*LoggerCtx, BS_LOGCUTTER,
                        VDISKP(VCtx->VDiskLogPrefix,
                            "KEEPER: RetryCutLogEvent: no progress limit exceeded; FreeUpToLsn# %" PRIu64
                            " firstLsnToKeep# %" PRIu64 " lastFirstLsnToKeep# %" PRIu64,
                            FreeUpToLsn, firstLsnToKeep, LastCutLogRetryFirstLsnToKeep));
                return;
            }

            LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: RetryCutLogEvent: retried; FreeUpToLsn# %" PRIu64
                        " firstLsnToKeep# %" PRIu64 " retriesWithoutProgress# %" PRIu32,
                        FreeUpToLsn, firstLsnToKeep, CutLogRetries));

            // retry event with old value of FreeUpToLsn
            DelayedActions.SetCutLog();
        }

        void TSyncLogKeeperState::FreeChunkEvent(ui32 chunkIdx) {
            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: TEvSyncLogFreeChunk: chunkIdx# %" PRIu32,
                        chunkIdx));

            ChunksToDeleteDelayed.Erase(chunkIdx);
            ChunksToDelete.push_back(chunkIdx);
            DelayedActions.SetDeleteChunk();
        }

        bool TSyncLogKeeperState::CutLogActionRequiresCommit() const {
            return DelayedActions.HasCutLog() && CalculateFirstLsnToKeep() < FreeUpToLsn;
        }

        bool TSyncLogKeeperState::PerformCutLogAction(std::function<void(ui64)> &&notCommitHandler) {
            if (DelayedActions.HasCutLog()) {
                DelayedActions.ClearCutLog();
            } else {
                return false;
            }

            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            //
            //       -----------  FreeUpToLsn (excluding)
            //                 \/
            //       -------------------------------------------------> time (lsn)
            //                  ^
            //                  |---------- FirstLsnToKeep
            //
            // ok = firstLsnToKeep >= FreeUpToLsn;
            //
            // if commit == false => no action required
            // if commit == true  => we will do all work in FixMemoryAndDiskOverflow based on FreeUpToLsn

            const bool commit = firstLsnToKeep < FreeUpToLsn;
            if (!commit) {
                notCommitHandler(firstLsnToKeep);
            }

            LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: PerformCutLogAction: commit# %d decomposed# %s",
                        int(commit), CalculateFirstLsnToKeepDecomposed().data()));

            return commit;
        }

        bool TSyncLogKeeperState::PerformCutLogActionIfNoCommit(std::function<void(ui64)> &&notCommitHandler) {
            if (!DelayedActions.HasCutLog()) {
                return false;
            }

            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            const bool commit = firstLsnToKeep < FreeUpToLsn;
            if (commit) {
                return false;
            }

            DelayedActions.ClearCutLog();
            notCommitHandler(firstLsnToKeep);

            LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: PerformCutLogActionIfNoCommit: decomposed# %s",
                        CalculateFirstLsnToKeepDecomposed().data()));

            return true;
        }

        bool TSyncLogKeeperState::PerformTrimTailAction() {
            if (DelayedActions.HasTrimTail()) {
                DelayedActions.ClearTrimTail();
            } else {
                return false;
            }

            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "KEEPER: cut log: TrimTailLsn# %" PRIu64
                        " ChunksToDeleteDelayed# %s", TrimTailLsn,
                        ChunksToDeleteDelayed.ToString().data()));

            // If TrimTailLsn is outdated, we just ignore it and log it,
            // SynclogKeeper can decide to cut log by some other reason.
            // Currently the only one reason for now is log was truncated
            // as being too large.
            auto logger = [this] (const TString &msg) {
                LOG_INFO(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(VCtx->VDiskLogPrefix, "KEEPER: %s", msg.data()));
            };

            TVector<ui32> scheduledChunks = SyncLogPtr->TrimLogByConfirmedLsn(TrimTailLsn, Notifier, logger);
            ChunksToDeleteDelayed.Insert(scheduledChunks);

            // we don't need to commit because we either remove mem pages or
            // schedule to remove some chunks (but they may be used by snapshots,
            // so wait until TEvSyncLogFreeChunk message)
            return false;
        }

        bool TSyncLogKeeperState::PerformMemOverflowAction() {
            if (DelayedActions.HasMemOverflow()) {
                DelayedActions.ClearMemOverflow();
            } else {
                return false;
            }

            // if not enough memory try to remove cache pages
            bool memOverflow = SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages;
            if (memOverflow) {
                ui64 lsn = CalculateFirstDataInRecovLogLsnToKeep();
                SyncLogPtr->RemoveCachedPages(MaxMemPages, lsn);
            }

            // if we still have memory overflow, perform write swap to disk
            bool stillMemOverflow = SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages;
            return stillMemOverflow;
        }

        bool TSyncLogKeeperState::PerformDeleteChunkAction() {
            if (DelayedActions.HasDeleteChunk()) {
                DelayedActions.ClearDeleteChunk();
            } else {
                return false;
            }

            // yes, log new entry point, if we have some chunks ready
            // for returning to PDisk
            return !ChunksToDelete.empty();
        }

        bool TSyncLogKeeperState::PerformInitialCommit() {
            return std::exchange(NeedsInitialCommit, false);
        }

        TSyncLogKeeperDebugInfo TSyncLogKeeperState::GetDebugInfo(bool commitInProgress) const {
            TSyncLogKeeperDebugInfo info;
            info.FirstLsnToKeep = CalculateFirstLsnToKeep();
            info.FreeUpToLsn = FreeUpToLsn;
            info.LastCutLogRetryFirstLsnToKeep = LastCutLogRetryFirstLsnToKeep;
            info.LastCommitEntryPointLsn = LastCommit.EntryPointLsn;
            info.LastCommitRecoveryLogConfirmedLsn = LastCommit.RecoveryLogConfirmedLsn;
            info.DiskLastLsn = SyncLogPtr->GetDiskLastLsn();
            info.LastLsn = SyncLogPtr->GetLastLsn();
            info.MemPages = SyncLogPtr->GetNumberOfPagesInMemory();
            info.MaxMemPages = MaxMemPages;
            info.DiskChunks = SyncLogPtr->GetSizeInChunks();
            info.MaxDiskChunks = MaxDiskChunks;
            info.CutLogRetries = CutLogRetries;
            info.FreeUpToLsnSatisfied = FreeUpToLsnSatisfied();
            info.HasDelayedActions = HasDelayedActions();
            info.CommitInProgress = commitInProgress;
            if (VCtx->IsLogRescueMode()) {
                info.NextCommitPlan = BuildNextCommitPlan();
            }
            info.LastSwap = LastSwapDebugInfo;
            info.LastCommitAttempt = LastCommitDebugInfo;
            info.FirstLsnToKeepDecomposed = CalculateFirstLsnToKeepDecomposed();
            return info;
        }

        TSyncLogKeeperCommitData TSyncLogKeeperState::PrepareCommitData(ui64 recoveryLogConfirmedLsn) {
            // we _copy_ ChunksToDeleteDelayed and _move_ ChunksToDelete

            // fix mem and disk overflow
            TMemRecLogSnapshotPtr swapSnap = FixMemoryAndDiskOverflow();
            // take snap after trimming live DiskRecLog; otherwise the entry point can serialize chunks that were
            // just moved to ChunksToDeleteDelayed by FixDiskOverflow().
            TSyncLogSnapshotPtr syncLogSnap = SyncLogPtr->GetSnapshot();
            // copy from TSet to vector
            TVector<ui32> deleteDelayed = ChunksToDeleteDelayed.Copy();

            // NOTE: if there is no updates going to SyncLog (and recovery log respectively),
            // recoveryLogConfirmedLsn can be very old and pessimistic. And more important, it doens't
            // change in time, because there is updates. That means that we can't cut log even if
            // we write a new entry point! We refine recoveryLogConfirmedLsn by taking into account
            // current entry point.
            // Proof that it's correct. Entry point lsn is already written, it means, that we received all
            // records before this lsn. We take into account these records for new entry point.
            const ui64 refinedRecoveryLogConfirmedLsn = Max(LastCommit.EntryPointLsn, recoveryLogConfirmedLsn);

            TSyncLogKeeperCommitData result(
                    std::move(syncLogSnap),
                    std::move(swapSnap),
                    std::move(deleteDelayed),
                    std::move(ChunksToDelete),
                    refinedRecoveryLogConfirmedLsn);

            return result;
        }

        void TSyncLogKeeperState::RecordCommitAttempt(const TSyncLogKeeperCommitData& commitData, TInstant now) {
            const ui64 seqNo = LastCommitDebugInfo.AttemptSeqNo + 1;
            LastCommitDebugInfo = {};
            LastCommitDebugInfo.AttemptSeqNo = seqNo;
            LastCommitDebugInfo.AttemptTime = now;
            LastCommitDebugInfo.AttemptSwapSnapPages = commitData.SwapSnap ? commitData.SwapSnap->Size() : 0;
            LastCommitDebugInfo.AttemptChunksToDeleteDelayed = commitData.ChunksToDeleteDelayed.size();
            LastCommitDebugInfo.AttemptChunksToDelete = commitData.ChunksToDelete.size();
            LastCommitDebugInfo.AttemptRecoveryLogConfirmedLsn = commitData.RecoveryLogConfirmedLsn;
            LastCommitDebugInfo.AttemptSwapSnapBoundaries = commitData.SwapSnap ?
                commitData.SwapSnap->BoundariesToString() : "<empty>";
            LastCommitDebugInfo.Status = "InProgress";
        }

        ui64 TSyncLogKeeperState::ApplyCommitResult(TEvSyncLogCommitDone *msg) {
            // apply all appends to DiskRecLog and update dbg info
            SyncLogPtr->UpdateDiskIndex(msg->Delta, msg->EntryPointDbgInfo);
            // save last commit info
            LastCommit = msg->CommitInfo;
            LastCommitDebugInfo.Status = "OK";
            LastCommitDebugInfo.ResultSeqNo = LastCommitDebugInfo.AttemptSeqNo;
            LastCommitDebugInfo.ResultTime = msg->CommitInfo.Time;
            LastCommitDebugInfo.ResultEntryPointLsn = msg->CommitInfo.EntryPointLsn;
            LastCommitDebugInfo.ResultRecoveryLogConfirmedLsn = msg->CommitInfo.RecoveryLogConfirmedLsn;
            LastCommitDebugInfo.ResultSwapAppends = msg->Delta.AllAppends.size();
            for (const auto& append : msg->Delta.AllAppends) {
                LastCommitDebugInfo.ResultSwapPages += append.Pages.size();
            }
            LastCommitDebugInfo.ResultEntryPointDbgInfo = msg->EntryPointDbgInfo.ToString();

            const TEntryPointDbgInfo &info = SyncLogPtr->GetLastEntryPointDbgInfo();
            if (info.ByteSize > SyncLogMaxEntryPointSize) {
                LOG_ERROR(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(VCtx->VDiskLogPrefix,
                            "KEEPER: last entry point size "
                            "is too large; LastEntryPointDbgInfo# %s "
                            "SyncLogMaxEntryPointSize# %" PRIu64,
                            info.ToString().data(), SyncLogMaxEntryPointSize));
            }
            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages) {
                DelayedActions.SetMemOverflow();
            }
            return firstLsnToKeep;
        }

        // Fix Disk overflow, i.e. remove some chunks from SyncLog
        TVector<ui32> TSyncLogKeeperState::FixDiskOverflow(ui32 numChunksToAdd) {
            // prepare disk write
            const ui32 numCurChunks = SyncLogPtr->GetSizeInChunks();
            bool diskOverflow = (numCurChunks + numChunksToAdd) > MaxDiskChunks;
            if (numCurChunks && diskOverflow) {
                // NOTE: we can swap many chunks, more than MaxDiskChunks, we have seen this
                const ui32 numChunksToDel = numCurChunks + numChunksToAdd - MaxDiskChunks;
                // returns chunk ids we can schedule for deletion;
                // we can't delete them right now, because they can be used by snapshot
                return SyncLogPtr->TrimLogByRemovingChunks(numChunksToDel, Notifier);
            } else {
                return {};
            }
        }

        TMemRecLogSnapshotPtr TSyncLogKeeperState::BuildSwapSnapDebugInfo(TSyncLogKeeperSwapDebugInfo& info) const {
            // find mem pages to write to disk
            const bool stillMemOverflow = SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages;
            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            const bool wantToCutRecoveryLog = FreeUpToLsn > firstLsnToKeep;
            info = {};
            info.Attempted = stillMemOverflow || wantToCutRecoveryLog;
            info.WantToCutRecoveryLog = wantToCutRecoveryLog;
            info.StillMemOverflow = stillMemOverflow;

            TMemRecLogSnapshotPtr swapSnap;
            if (stillMemOverflow || wantToCutRecoveryLog) {
                // we write those records, that are not written to disk yet
                const ui64 diskLastLsn = SyncLogPtr->GetDiskLastLsn();
                // free pages in case of memory overflow
                const ui32 freeNPages = stillMemOverflow ?
                    SyncLogPtr->GetNumberOfPagesInMemory() - MaxMemPages : 0;

                // if wantToCutRecoveryLog, then FreeUpToLsn must > 0
                Y_ABORT_UNLESS(!wantToCutRecoveryLog || (FreeUpToLsn > 0));
                const ui64 freeUpToLsn = wantToCutRecoveryLog ? FreeUpToLsn : 0;

                const ui32 pagesInChunk = SyncLogPtr->GetChunkSize() / SyncLogPtr->GetAppendBlockSize();
                const ui32 maxSwapPages = Max<ui32>(1, Min(MaxMemPages, pagesInChunk));
                info.DiskLastLsn = diskLastLsn;
                info.FreeUpToLsn = freeUpToLsn;
                info.FreeNPages = freeNPages;
                info.MaxSwapPages = maxSwapPages;

                // build swap snap
                swapSnap = SyncLogPtr->BuildMemSwapSnapshot(diskLastLsn, freeUpToLsn, freeNPages, maxSwapPages);
                info.SwapSnapPages = swapSnap ? swapSnap->Size() : 0;
                info.SwapSnapBoundaries = swapSnap ? swapSnap->BoundariesToString() : "{Mem: empty}";
            }

            return swapSnap;
        }

        TSyncLogKeeperCommitPlanDebugInfo TSyncLogKeeperState::BuildNextCommitPlan() const {
            TSyncLogKeeperCommitPlanDebugInfo info;
            info.TrimTailPending = DelayedActions.HasTrimTail();
            info.CutLogCommitRequired = CutLogActionRequiresCommit();
            info.MemOverflowCommitRequired = SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages;
            info.DeleteChunkCommitRequired = !ChunksToDelete.empty();
            info.InitialCommitRequired = NeedsInitialCommit;
            info.HasWork = info.TrimTailPending || info.CutLogCommitRequired || info.MemOverflowCommitRequired ||
                info.DeleteChunkCommitRequired || info.InitialCommitRequired;
            info.CurrentDiskChunks = SyncLogPtr->GetSizeInChunks();
            info.ChunksToDeleteDelayed = ui32(ChunksToDeleteDelayed.Get().size());
            info.ChunksToDeleteReady = ui32(ChunksToDelete.size());

            const TMemRecLogSnapshotPtr swapSnap = BuildSwapSnapDebugInfo(info.Swap);
            info.ChunksToAdd = SyncLogPtr->HowManyChunksAdds(swapSnap);
            if (info.CurrentDiskChunks + info.ChunksToAdd > MaxDiskChunks) {
                info.ChunksToTrimForQuota = info.CurrentDiskChunks + info.ChunksToAdd - MaxDiskChunks;
            }
            return info;
        }

        TMemRecLogSnapshotPtr TSyncLogKeeperState::BuildSwapSnap() {
            TMemRecLogSnapshotPtr swapSnap = BuildSwapSnapDebugInfo(LastSwapDebugInfo);
            if (LastSwapDebugInfo.Attempted) {
                LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                        VDISKP(VCtx->VDiskLogPrefix,
                            "KEEPER: BuildSwapSnap: wantToCutRecoveryLog# %" PRIu32
                            " stillMemOverflow# %" PRIu32 " diskLastLsn# %" PRIu64
                            " freeUpToLsn# %" PRIu64 " freeNPages# %" PRIu32
                            " maxSwapPages# %" PRIu32 " swapSnap# %s",
                            ui32(LastSwapDebugInfo.WantToCutRecoveryLog),
                            ui32(LastSwapDebugInfo.StillMemOverflow),
                            LastSwapDebugInfo.DiskLastLsn, LastSwapDebugInfo.FreeUpToLsn,
                            LastSwapDebugInfo.FreeNPages, LastSwapDebugInfo.MaxSwapPages,
                            LastSwapDebugInfo.SwapSnapBoundaries.data()));
            }
            return swapSnap;
        }

        TMemRecLogSnapshotPtr TSyncLogKeeperState::FixMemoryAndDiskOverflow() {
            // build a bunch of memory pages to swap to disk
            TMemRecLogSnapshotPtr swapSnap = BuildSwapSnap();
            // find out how many new chunks we must add
            ui32 numChunksToAdd = SyncLogPtr->HowManyChunksAdds(swapSnap);
            // get current number of allocated chunks for SyncLog
            ui32 numCurChunks = SyncLogPtr->GetSizeInChunks();

            // if memory synclog is enormously large (hmm, we've seen this after
            // multiple unsuccessful attempts to recover), we want to save to disk
            // too much data and overflow quota, log this event at least
            if (numChunksToAdd > MaxDiskChunks) {
                LOG_ERROR(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(VCtx->VDiskLogPrefix,
                            "KEEPER: we've got disk overflow for SyncLog:"
                            " numCurChunks# %" PRIu32 " numChunksToAdd# %" PRIu32
                            " MaxDiskChunks# %" PRIu32, numCurChunks, numChunksToAdd, MaxDiskChunks));
            }

            // trim SyncLog in case of disk overflow
            TVector<ui32> scheduledChunks = FixDiskOverflow(numChunksToAdd);
            // append scheduledChunks to ChunksToDeleteDelayed
            ChunksToDeleteDelayed.Insert(scheduledChunks);

            return swapSnap;
        }

        ui64 TSyncLogKeeperState::CalculateFirstDataInRecovLogLsnToKeep() const {
            // what SyncLog thinks about first lsn (including LogStartLsn)
            ui64 firstSyncLogLsnToKeep = SyncLogPtr->FirstLsnToKeep();
            // we don't need records before TrimTailLsn, so take Max
            ui64 firstDataInRecovLogLsnToKeep = Max(TrimTailLsn, firstSyncLogLsnToKeep);
            return firstDataInRecovLogLsnToKeep;
        }

        void TSyncLogKeeperState::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) {
            auto process = [&](const auto& m) {
                for (const TChunkIdx chunkId : m) {
                    if (chunksOfInterest.contains(chunkId)) {
                        chunks.insert(chunkId);
                    }
                }
            };

            process(ChunksToDelete);
            process(ChunksToDeleteDelayed.Get());

            TSet<ui32> temp;
            SyncLogPtr->GetOwnedChunks(temp);
            process(temp);
        }

    } // NSyncLog
} // NKikimr
