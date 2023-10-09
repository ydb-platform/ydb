#pragma once

#include "defs.h"

#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogrecovery.h"
#include "blobstorage_synclogkeeper_committer.h"

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TChunksToDeleteDelayed
        ////////////////////////////////////////////////////////////////////////////
        class TChunksToDeleteDelayed {
        public:
            void Insert(const TVector<ui32> &chunks) {
                for (auto chunkIdx : chunks) {
                    bool inserted = Chunks.insert(chunkIdx).second;
                    Y_ABORT_UNLESS(inserted);
                }
            }

            TVector<ui32> Copy() const {
                // copy from TSet to vector
                TVector<ui32> vec;
                vec.assign(Chunks.begin(), Chunks.end());
                return vec;
            }

            void Erase(ui32 chunkIdx) {
                const ui32 del = Chunks.erase(chunkIdx);
                Y_ABORT_UNLESS(del == 1);
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

            void Output(IOutputStream &s) const {
                s << "[";
                bool first = true;
                for (auto chunkIdx : Chunks) {
                    if (first)
                        first = false;
                    else
                        s << " ";
                    s << chunkIdx;
                }
                s << "]";
            }

        private:
            TSet<ui32> Chunks;
        };



        ////////////////////////////////////////////////////////////////////////////
        // TDelayedActions
        ////////////////////////////////////////////////////////////////////////////
        class TDelayedActions {
        public:
            // Set methods
            void SetTrimTail()          { TrimTailAction = true; }
            void SetCutLog()            { CutLogAction = true; }
            void SetMemOverflow()       { MemOverflowAction = true; }
            void SetDeleteChunk()       { DeleteChunkAction = true; }
            // Has methods
            bool HasTrimTail()          { return TrimTailAction; }
            bool HasCutLog()            { return CutLogAction; }
            bool HasMemOverflow()       { return MemOverflowAction; }
            bool HasDeleteChunk()       { return DeleteChunkAction; }
            // Clear methods
            void ClearTrimTail()        { TrimTailAction = false; }
            void ClearCutLog()          { CutLogAction = false; }
            void ClearMemOverflow()     { MemOverflowAction = false; }
            void ClearDeleteChunk()     { DeleteChunkAction = false; }

            // Has any action?
            bool HasActions() const {
                return TrimTailAction || CutLogAction || MemOverflowAction || DeleteChunkAction;
            }

        private:
            bool TrimTailAction = false;
            bool CutLogAction = false;
            bool MemOverflowAction = false;
            bool DeleteChunkAction = false;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogKeeperState
        // Manages entry point and commits additionally to TSyncLog
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogKeeperState {
        public:
            TSyncLogKeeperState(
                    TIntrusivePtr<TVDiskContext> vctx,
                    std::unique_ptr<TSyncLogRepaired> repaired,
                    ui64 syncLogMaxMemAmount,
                    ui64 syncLogMaxDiskAmount,
                    ui64 syncLogMaxEntryPointSize);

            void Init(std::shared_ptr<IActorNotify> notifier, std::shared_ptr<ILoggerCtx> loggerCtx) {
                Notifier = std::move(notifier);
                LoggerCtx = std::move(loggerCtx);
            }

            bool HasDelayedActions() const {
                return DelayedActions.HasActions();
            }

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
            void BaldLogEvent();
            void CutLogEvent(ui64 freeUpToLsn);
            void RetryCutLogEvent();
            void FreeChunkEvent(ui32 chunkIdx);

            // action performers
            bool PerformCutLogAction(std::function<void(ui64)> &&notCommitHandler);
            // just trim log based by TrimTailLsn (which is confirmed lsn from peers)
            bool PerformTrimTailAction();
            bool PerformMemOverflowAction();
            bool PerformDeleteChunkAction();
            bool PerformInitialCommit();

            bool FreeUpToLsnSatisfied() const { return CalculateFirstLsnToKeep() >= FreeUpToLsn; }
            TSyncLogKeeperCommitData PrepareCommitData(ui64 recoveryLogConfirmedLsn);
            // applies commit result and returns first lsn to keep
            ui64 ApplyCommitResult(TEvSyncLogCommitDone *msg);

        private:
            // VDisk Context
            TIntrusivePtr<TVDiskContext> VCtx;
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
            // chunks that are still used by snapshots
            TChunksToDeleteDelayed ChunksToDeleteDelayed;
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

            // Fix Disk overflow, i.e. remove some chunks from SyncLog
            TVector<ui32> FixDiskOverflow(ui32 numChunksToAdd);
            // Build Snapshot of memory pages for swapping to disk
            TMemRecLogSnapshotPtr BuildSwapSnap();
            // We have limits on
            // 1. RAM used for SyncLog
            // 2. Disk chunks used for SyncLog
            // The function fixes limitation excess by
            // 1. returning swapSnap to write to disk (frees memory)
            // 2. removing some old chunks (putting them to ChunksToDeleteDelayed)
            TMemRecLogSnapshotPtr FixMemoryAndDiskOverflow();
            // Calculate first lsn to keep in recovery log for _DATA_RECORDS_,
            // i.e. for those records in SyncLog which keep user data
            ui64 CalculateFirstDataInRecovLogLsnToKeep() const;
        };

    } // NSyncLog
} // NKikimr

