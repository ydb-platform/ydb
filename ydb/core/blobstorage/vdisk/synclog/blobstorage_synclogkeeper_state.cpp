#include "blobstorage_synclogkeeper_state.h"

#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_builder.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

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
                TIntrusivePtr<TSyncLogCtx> slCtx,
                std::unique_ptr<TSyncLogRepaired> repaired,
                ui64 syncLogMaxMemAmount,
                ui64 syncLogMaxDiskAmount,
                ui64 syncLogMaxEntryPointSize)
            : SlCtx(std::move(slCtx))
            , SyncLogPtr(std::move(repaired->SyncLogPtr))
            , ChunksToDelete(std::move(repaired->ChunksToDelete))
            , LastCommit(repaired->CommitHistory)
            , MaxMemPages(CalcMaxMemPages(syncLogMaxMemAmount, SyncLogPtr->GetAppendBlockSize()))
            , MaxDiskChunks(CalcMaxDiskChunks(syncLogMaxDiskAmount, SyncLogPtr->GetChunkSize()))
            , SyncLogMaxEntryPointSize(syncLogMaxEntryPointSize)
            , NeedsInitialCommit(repaired->NeedsInitialCommit)
            , PhantomFlagStorageState(SlCtx)
            , EnablePhantomFlagStorage(SlCtx->EnablePhantomFlagStorage)
            , PhantomFlagStorageLimit(SlCtx->PhantomFlagStorageLimit)
            , SelfOrderNumber(SlCtx->VCtx->Top->GetOrderNumber(SlCtx->VCtx->ShortSelfVDisk))
        {
            Y_VERIFY_S(SlCtx->VCtx->Top->GetTotalVDisksNum() <= MaxPossibleDisksInGroup,
                    "Bad erasure# " << TBlobStorageGroupType::ErasureSpeciesName(SlCtx->VCtx->Top->GType.GetErasure()));
            SyncedLsns.resize(SlCtx->VCtx->Top->GetTotalVDisksNum());
            SyncedLsns[SelfOrderNumber] = Max<ui64>();
            SyncedMask.reset();
            SyncedMask.set(SelfOrderNumber, true);
        }

        // Calculate first lsn in recovery log we must keep
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
            // TODO: put blobs and hard barriers in phantom flag storage thresholds
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.MemOverflow = true;

            // Put blob record to PhantomFlagStorage
            if (PhantomFlagStorageState.IsActive() && rec->RecType == TRecordHdr::RecLogoBlob) {
                PhantomFlagStorageState.ProcessBlobRecordFromSyncLog(rec->GetLogoBlob(),
                        PhantomFlagStorageLimit);
            }
        }

        void TSyncLogKeeperState::PutMany(const void *buf, ui32 size) {
            SyncLogPtr->PutMany(buf, size);
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.MemOverflow = true;

            // Put all blob records to PhantomFlagStorage
            TRecordHdr* rec = (TRecordHdr*)buf;
            ui32 recSize = 0;
            do {
                recSize = rec->GetSize();
                Y_DEBUG_ABORT_UNLESS(recSize <= size);
                if (PhantomFlagStorageState.IsActive() && rec->RecType == TRecordHdr::RecLogoBlob) {
                    PhantomFlagStorageState.ProcessBlobRecordFromSyncLog(rec->GetLogoBlob(),
                            PhantomFlagStorageLimit);
                }
                rec = rec->Next();
                size -= recSize;
            } while (size);
        }

        // put the whole level into SyncLog
        void TSyncLogKeeperState::PutLevelSegment(const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> *seg) {
            char buffer[MaxRecFullSize];
            TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TMemIterator it(seg);
            ui64 lsn = seg->Info.FirstLsn;
            it.SeekToFirst();
            while (it.Valid()) {
                ui32 len = TSerializeRoutines::SetLogoBlob(SlCtx->VCtx->Top->GType,
                        buffer,
                        lsn,
                        it.GetCurKey().LogoBlobID(),
                        it.GetMemRec().GetIngress());
                Y_DEBUG_ABORT_UNLESS(len <= sizeof(buffer));
                SyncLogPtr->PutOne(reinterpret_cast<const TRecordHdr *>(buffer), len);
                it.Next();
                ++lsn;
            }
            Y_VERIFY_S(lsn <= seg->Info.LastLsn + 1, SlCtx->VCtx->VDiskLogPrefix);
            // Check for memory overflow
            if (SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages)
                DelayedActions.MemOverflow = true;
        }

        void TSyncLogKeeperState::TrimTailEvent(ui64 trimTailLsn) {
            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                        "KEEPER: TEvSyncLogTrim: trimLsn# %" PRIu64, trimTailLsn));

            TrimTailLsn = trimTailLsn;
            DelayedActions.TrimTail = true;
        }

        void TSyncLogKeeperState::BaldLogEvent(bool dropChunksExplicitly) {
            if (dropChunksExplicitly) {
                TSyncLogSnapshotPtr snapshot = SyncLogPtr->GetSnapshot();
                const ui32 numCurChunks = SyncLogPtr->GetSizeInChunks();
                if (numCurChunks > 0) {
                    TVector<ui32> droppedChunks = SyncLogPtr->TrimLogByRemovingChunks(numCurChunks, Notifier);
                    DropUnsyncedChunks(droppedChunks, snapshot);
                }

                LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: TEvSyncLogBaldLog DropChunksExplicitly: numChunks# %" PRIu32,
                            numCurChunks));
            } else {
                const ui64 baldLsn = SyncLogPtr->GetLastLsn();
                LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: TEvSyncLogBaldLog: baldLsn# %" PRIu64, baldLsn));
    
                TrimTailLsn = baldLsn;
                DelayedActions.TrimTail = true;
            }
        }

        void TSyncLogKeeperState::CutLogEvent(ui64 freeUpToLsn) {
            LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                    VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                        "KEEPER: NPDisk::TEvCutLog: freeUpToLsn# %" PRIu64, freeUpToLsn));

            FreeUpToLsn = freeUpToLsn;
            CutLogRetries = 0;
            DelayedActions.CutLog = true;
        }

        void TSyncLogKeeperState::RetryCutLogEvent() {
            ++CutLogRetries;
            if (CutLogRetries > 2) {
                // error condition, retry doens't help
                LOG_ERROR(*LoggerCtx, BS_LOGCUTTER,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: RetryCutLogEvent: limit exceeded; FreeUpToLsn# %" PRIu64, FreeUpToLsn));
            } else {
                LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: RetryCutLogEvent: retried; FreeUpToLsn# %" PRIu64, FreeUpToLsn));

                // retry event with old value of FreeUpToLsn
                DelayedActions.CutLog = true;
            }
        }

        void TSyncLogKeeperState::FreeChunkEvent(ui32 chunkIdx) {
            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG, VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                "KEEPER: TEvSyncLogFreeChunk: chunkIdx# %" PRIu32, chunkIdx));
            const size_t n = DeletedChunksPending.erase(chunkIdx);
            Y_ABORT_UNLESS(n == 1);
            if (DeletedChunks.erase(chunkIdx)) {
                ChunksToForget.push_back(chunkIdx);
            } else { // we haven't committed this chunk deletion yet, postpone its forgetting
                const auto [it, inserted] = ChunksToForgetPending.insert(chunkIdx);
                Y_ABORT_UNLESS(inserted);
            }
        }

        bool TSyncLogKeeperState::PerformCutLogAction(std::function<void(ui64)> &&notCommitHandler) {
            if (DelayedActions.CutLog) {
                DelayedActions.CutLog = false;
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
                    VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                        "KEEPER: PerformCutLogAction: commit# %d decomposed# %s",
                        int(commit), CalculateFirstLsnToKeepDecomposed().data()));

            return commit;
        }

        bool TSyncLogKeeperState::PerformTrimTailAction() {
            if (DelayedActions.TrimTail) {
                DelayedActions.TrimTail = false;
            } else {
                return false;
            }

            LOG_DEBUG(*LoggerCtx, BS_SYNCLOG,
                    VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                        "KEEPER: cut log: TrimTailLsn# %" PRIu64
                        " ChunksToDelete# %s", TrimTailLsn,
                        FormatList(ChunksToDelete).data()));

            // If TrimTailLsn is outdated, we just ignore it and log it,
            // SynclogKeeper can decide to cut log by some other reason.
            // Currently the only one reason for now is log was truncated
            // as being too large.
            auto logger = [this] (const TString &msg) {
                LOG_INFO(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix, "KEEPER: %s", msg.data()));
            };

            TVector<ui32> scheduledChunks = SyncLogPtr->TrimLogByConfirmedLsn(TrimTailLsn, Notifier, logger);
            ChunksToDelete.insert(ChunksToDelete.end(), scheduledChunks.begin(), scheduledChunks.end());
            DeletedChunksPending.insert(scheduledChunks.begin(), scheduledChunks.end());
            return !ChunksToDelete.empty();
        }

        bool TSyncLogKeeperState::PerformMemOverflowAction() {
            if (DelayedActions.MemOverflow) {
                DelayedActions.MemOverflow = false;
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

        bool TSyncLogKeeperState::PerformInitialCommit() {
            return std::exchange(NeedsInitialCommit, false);
        }

        TSyncLogKeeperCommitData TSyncLogKeeperState::PrepareCommitData(ui64 recoveryLogConfirmedLsn) {
            // take snap
            TSyncLogSnapshotPtr syncLogSnapshot = SyncLogPtr->GetSnapshot();
            // fix mem and disk overflow
            TMemRecLogSnapshotPtr swapSnap = FixMemoryAndDiskOverflow(syncLogSnapshot);

            // NOTE: if there is no updates going to SyncLog (and recovery log respectively),
            // recoveryLogConfirmedLsn can be very old and pessimistic. And more important, it doens't
            // change in time, because there is updates. That means that we can't cut log even if
            // we write a new entry point! We refine recoveryLogConfirmedLsn by taking into account
            // current entry point.
            // Proof that it's correct. Entry point lsn is already written, it means, that we received all
            // records before this lsn. We take into account these records for new entry point.
            const ui64 refinedRecoveryLogConfirmedLsn = Max(LastCommit.EntryPointLsn, recoveryLogConfirmedLsn);

            TSyncLogKeeperCommitData result(
                    std::move(syncLogSnapshot),
                    std::move(swapSnap),
                    std::exchange(ChunksToDelete, {}),
                    refinedRecoveryLogConfirmedLsn);
            return result;
        }

        ui64 TSyncLogKeeperState::ApplyCommitResult(TEvSyncLogCommitDone *msg) {
            // apply all appends to DiskRecLog and update dbg info
            SyncLogPtr->UpdateDiskIndex(msg->Delta, msg->EntryPointDbgInfo);
            // save last commit info
            LastCommit = msg->CommitInfo;

            for (ui32 chunkIdx : msg->ChunksDeleted) {
                if (ChunksToForgetPending.erase(chunkIdx)) { // this chunk has been released while commit was in flight
                    ChunksToForget.push_back(chunkIdx);
                } else {
                    const auto [it, inserted] = DeletedChunks.insert(chunkIdx);
                    Y_ABORT_UNLESS(inserted);
                }
            }

            const TEntryPointDbgInfo &info = SyncLogPtr->GetLastEntryPointDbgInfo();
            if (info.ByteSize > SyncLogMaxEntryPointSize) {
                LOG_ERROR(*LoggerCtx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: last entry point size "
                            "is too large; LastEntryPointDbgInfo# %s "
                            "SyncLogMaxEntryPointSize# %" PRIu64,
                            info.ToString().data(), SyncLogMaxEntryPointSize));
            }
            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
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

        TMemRecLogSnapshotPtr TSyncLogKeeperState::BuildSwapSnap() {
            // find mem pages to write to disk
            const bool stillMemOverflow = SyncLogPtr->GetNumberOfPagesInMemory() > MaxMemPages;
            const ui64 firstLsnToKeep = CalculateFirstLsnToKeep();
            const bool wantToCutRecoveryLog = FreeUpToLsn > firstLsnToKeep;

            TMemRecLogSnapshotPtr swapSnap;
            if (stillMemOverflow || wantToCutRecoveryLog) {
                // we write those records, that are not written to disk yet
                const ui64 diskLastLsn = SyncLogPtr->GetDiskLastLsn();
                // free pages in case of memory overflow
                const ui32 freeNPages = stillMemOverflow ?
                    SyncLogPtr->GetNumberOfPagesInMemory() - MaxMemPages : 0;

                // if wantToCutRecoveryLog, then FreeUpToLsn must > 0
                Y_VERIFY_S(!wantToCutRecoveryLog || (FreeUpToLsn > 0), SlCtx->VCtx->VDiskLogPrefix);
                const ui64 freeUpToLsn = wantToCutRecoveryLog ? FreeUpToLsn : 0;

                // build swap snap
                swapSnap = SyncLogPtr->BuildMemSwapSnapshot(diskLastLsn, freeUpToLsn, freeNPages);
                LOG_DEBUG(*LoggerCtx, BS_LOGCUTTER,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: BuildSwapSnap: wantToCutRecoveryLog# %" PRIu32
                            " stillMemOverflow# %" PRIu32 " diskLastLsn# %" PRIu64
                            " freeUpToLsn# %" PRIu64 " freeNPages# %" PRIu32 " swapSnap# %s",
                            ui32(wantToCutRecoveryLog), ui32(stillMemOverflow),
                            diskLastLsn, freeUpToLsn, freeNPages,
                            (swapSnap ? swapSnap->BoundariesToString().data() : "{Mem: empty}")));
            }

            return swapSnap;
        }

        TMemRecLogSnapshotPtr TSyncLogKeeperState::FixMemoryAndDiskOverflow(const TSyncLogSnapshotPtr& snapshot) {
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
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: we've got disk overflow for SyncLog:"
                            " numCurChunks# %" PRIu32 " numChunksToAdd# %" PRIu32
                            " MaxDiskChunks# %" PRIu32, numCurChunks, numChunksToAdd, MaxDiskChunks));
            }

            // trim SyncLog in case of disk overflow
            TVector<ui32> scheduledChunks = FixDiskOverflow(numChunksToAdd);
            DropUnsyncedChunks(scheduledChunks, snapshot);

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

            TSet<ui32> temp;
            SyncLogPtr->GetOwnedChunks(temp);
            process(temp);
        }


        void TSyncLogKeeperState::UpdateNeighbourSyncedLsn(ui32 orderNumber, ui64 syncedLsn) {
            SyncedLsns[orderNumber] = std::max(SyncedLsns[orderNumber], syncedLsn);

            // If there are no unsynced disks left, deactivate the PhantomFlagStorage
            // and remove all records from it.
            // TODO: more precise pruning
            ui64 firstStoredLsn = SyncLogPtr->GetFirstLsn();
            if (PhantomFlagStorageState.IsActive()) {
                if (std::all_of(SyncedLsns.begin(), SyncedLsns.end(), [&](ui64 lsn) {
                    return lsn == Max<ui64>() || lsn + 1 >= firstStoredLsn;
                })) {
                    PhantomFlagStorageState.Deactivate();
                }
            }
        }

        void TSyncLogKeeperState::FinishPhantomFlagStorageBuilder(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds) {
            PhantomFlagStorageState.FinishBuilding(std::move(flags), std::move(thresholds), PhantomFlagStorageLimit);
        }

        TPhantomFlagStorageSnapshot TSyncLogKeeperState::GetPhantomFlagStorageSnapshot() const {
            return PhantomFlagStorageState.GetSnapshot();
        }

        void TSyncLogKeeperState::ProcessLocalSyncData(ui32 orderNumber, const TString& data) {
            PhantomFlagStorageState.ProcessLocalSyncData(orderNumber, data);
        }

        void TSyncLogKeeperState::DropUnsyncedChunks(const TVector<ui32>& chunks, const TSyncLogSnapshotPtr& snapshot) {
            ui64 firstStoredLsn = SyncLogPtr->GetFirstLsn();
            for (ui32 orderNumber = 0; orderNumber < SlCtx->VCtx->Top->GType.BlobSubgroupSize(); ++orderNumber) {
                bool synced = (orderNumber == SelfOrderNumber) || (SyncedLsns[orderNumber] + 1 >= firstStoredLsn);
                SyncedMask.set(orderNumber, synced);
            }

            if (EnablePhantomFlagStorage) {
                PhantomFlagStorageState.UpdateSyncedMask(SyncedMask);
                if (!chunks.empty() && !PhantomFlagStorageState.IsActive() && SelfId != TActorId{}) {
                    PhantomFlagStorageState.StartBuilding();
                    TActivationContext::Register(CreatePhantomFlagStorageBuilderActor(SlCtx, SelfId, snapshot));
                }
            }

            ChunksToDelete.insert(ChunksToDelete.end(), chunks.begin(), chunks.end());
            DeletedChunksPending.insert(chunks.begin(), chunks.end());
            if (!ChunksToDelete.empty()) {
                DelayedActions.DeleteChunk = true;
            }
        }

        void TSyncLogKeeperState::UpdateMetrics() {
            PhantomFlagStorageState.UpdateMetrics();
            SlCtx->PhantomFlagStorageGroup.SyncedMask() = SyncedMask.to_ullong();
        }

    } // NSyncLog
} // NKikimr
