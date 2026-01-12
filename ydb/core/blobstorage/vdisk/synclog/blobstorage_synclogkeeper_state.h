#pragma once

#include "defs.h"

#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogrecovery.h"
#include "blobstorage_synclogkeeper_committer.h"

#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flags.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_state.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_snapshot.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TDelayedActions
        ////////////////////////////////////////////////////////////////////////////
        struct TDelayedActions {
            // Has any action?
            bool HasActions() const {
                return TrimTail || CutLog || MemOverflow || DeleteChunk;
            }

            bool TrimTail = false;
            bool CutLog = false;
            bool MemOverflow = false;
            bool DeleteChunk = false;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogKeeperState
        // Manages entry point and commits additionally to TSyncLog
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogKeeperState {
        public:
            TSyncLogKeeperState(
                    TIntrusivePtr<TSyncLogCtx> slCtx,
                    std::unique_ptr<TSyncLogRepaired> repaired,
                    ui64 syncLogMaxMemAmount,
                    ui64 syncLogMaxDiskAmount,
                    ui64 syncLogMaxEntryPointSize);

            void Init(std::shared_ptr<IActorNotify> notifier, std::shared_ptr<ILoggerCtx> loggerCtx,
                    const TActorId& selfId) {
                Notifier = std::move(notifier);
                LoggerCtx = std::move(loggerCtx);
                SelfId = selfId;
            }

            bool HasDelayedActions() const { return DelayedActions.HasActions(); }
            bool GetDeleteChunkAndClear() { return std::exchange(DelayedActions.DeleteChunk, false); }

            TSyncLogSnapshotPtr GetSyncLogSnapshot() const {
                return SyncLogPtr->GetSnapshot();
            }

            void FillInSyncLogEssence(TLogEssence *e) const {
                SyncLogPtr->FillInLogEssence(e);
            }

            // Calculate first lsn in recovery log we must to keep
            ui64 CalculateFirstLsnToKeep() const;
            TString CalculateFirstLsnToKeepDecomposed() const;
            void PutOne(const TRecordHdr *rec, ui32 size);
            void PutMany(const void *buf, ui32 size);
            // put the whole level into SyncLog
            void PutLevelSegment(const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> *seg);

            // incoming events
            void TrimTailEvent(ui64 trimTailLsn);
            void BaldLogEvent(bool dropChunksExplicitly);
            void CutLogEvent(ui64 freeUpToLsn);
            void RetryCutLogEvent();
            void FreeChunkEvent(ui32 chunkIdx);

            // action performers
            bool PerformCutLogAction(std::function<void(ui64)> &&notCommitHandler);
            // just trim log based by TrimTailLsn (which is confirmed lsn from peers)
            bool PerformTrimTailAction();
            bool PerformMemOverflowAction();
            bool PerformInitialCommit();

            bool FreeUpToLsnSatisfied() const { return CalculateFirstLsnToKeep() >= FreeUpToLsn; }
            TSyncLogKeeperCommitData PrepareCommitData(ui64 recoveryLogConfirmedLsn);
            // applies commit result and returns first lsn to keep
            ui64 ApplyCommitResult(TEvSyncLogCommitDone *msg);

            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks);

            void UpdateNeighbourSyncedLsn(ui32 orderNumber, ui64 syncedLsn);

            // Add flags from cut sync log snapshot
            void FinishPhantomFlagStorageBuilder(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds);
            TPhantomFlagStorageSnapshot GetPhantomFlagStorageSnapshot() const;
            void ProcessLocalSyncData(ui32 orderNumber, const TString& data);

            void UpdateMetrics();

            TVector<ui32> GetChunksToForget() {
                return std::exchange(ChunksToForget, {});
            }

        private:
            // VDisk Context
            TIntrusivePtr<TSyncLogCtx> SlCtx;
            // SyncLog data structres
            TSyncLogPtr SyncLogPtr;
            // chunks we can and must delete for the next commit message
            TVector<ui32> ChunksToDelete;
            // last commit info: entry point lsn and time
            TCommitHistory LastCommit;
            // all vdisk peers synced with this lsn, i.e. confirmed lsn
            ui64 TrimTailLsn = 0;
            // last request to cut log from Yard/PDisk
            ui64 FreeUpToLsn = 0;
            // actions we must apply but have not applied yet
            TDelayedActions DelayedActions;
            // notifier for deleted chunks
            std::shared_ptr<IActorNotify> Notifier;
            // logger ctx
            std::shared_ptr<ILoggerCtx> LoggerCtx;
            // number of retries me made to cut the log
            ui32 CutLogRetries = 0;
            // settings:
            const ui32 MaxMemPages;
            const ui32 MaxDiskChunks;
            const ui64 SyncLogMaxEntryPointSize;
            // does it need initial commit?
            bool NeedsInitialCommit;
            // Id of Keeper actor which possesses the state
            TActorId SelfId;

            // synced lsns of neighbours
            std::vector<ui64> SyncedLsns;
            TSyncedMask SyncedMask;

            // phantom flag storage
            TPhantomFlagStorageState PhantomFlagStorageState;
            TMemorizableControlWrapper EnablePhantomFlagStorage;
            TMemorizableControlWrapper PhantomFlagStorageLimit;

            ui32 SelfOrderNumber;

            TVector<ui32> ChunksToForget;
            THashSet<ui32> ChunksToForgetPending;
            THashSet<ui32> DeletedChunks;
            THashSet<ui32> DeletedChunksPending;

        private:
            // Fix Disk overflow, i.e. remove some chunks from SyncLog
            TVector<ui32> FixDiskOverflow(ui32 numChunksToAdd);
            // Build Snapshot of memory pages for swapping to disk
            TMemRecLogSnapshotPtr BuildSwapSnap();
            // We have limits on
            // 1. RAM used for SyncLog
            // 2. Disk chunks used for SyncLog
            // The function fixes limitation excess by
            // 1. returning swapSnap to write to disk (frees memory)
            // 2. removing some old chunks (putting them to ChunksToDelete)
            TMemRecLogSnapshotPtr FixMemoryAndDiskOverflow(const TSyncLogSnapshotPtr& snapshot);
            // Calculate first lsn to keep in recovery log for _DATA_RECORDS_,
            // i.e. for those records in SyncLog which keep user data
            ui64 CalculateFirstDataInRecovLogLsnToKeep() const;
            // Schedule chunks deletion and activate PhantomFlagStorage if needed
            void DropUnsyncedChunks(const TVector<ui32>& chunks,
                    const TSyncLogSnapshotPtr& snapshot);
        };

    } // NSyncLog
} // NKikimr

