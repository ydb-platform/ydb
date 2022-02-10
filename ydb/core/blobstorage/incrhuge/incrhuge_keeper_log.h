#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"

#include <util/generic/hash_set.h>
#include <util/generic/set.h>

namespace NKikimrVDiskData {
    class TIncrHugeChunks;
} // NKikimrVDiskData

namespace NKikimr {
    namespace NIncrHuge {

        class TChunkRecordMerger {
            struct TChunkInfo {
                TChunkSerNum ChunkSerNum;
                NKikimrVDiskData::TIncrHugeChunks::EChunkState State;
            };

            THashMap<TChunkIdx, TChunkInfo> Chunks;
            TMaybe<TChunkSerNum> CurrentSerNum;

        public:
            struct TChunkAllocation {
                TVector<TChunkIdx> NewChunkIds;
                TChunkSerNum BaseSerNum;
                TChunkSerNum CurrentSerNum;
            };

            struct TChunkDeletion {
                TChunkIdx ChunkIdx;
                TChunkSerNum ChunkSerNum;
                ui32 NumItems;
            };

            struct TCompleteChunk {
                TChunkIdx ChunkIdx;
                TChunkSerNum ChunkSerNum;
            };

        public:
            void operator ()(const NKikimrVDiskData::TIncrHugeChunks& record);
            void operator ()(const TChunkAllocation& record);
            void operator ()(const TChunkDeletion& record);
            void operator ()(const TCompleteChunk& record);
            NKikimrVDiskData::TIncrHugeChunks GetCurrentState() const;

            static TString Serialize(const NKikimrVDiskData::TIncrHugeChunks& record);
            static TString Serialize(const TChunkAllocation& record);
            static TString Serialize(const TChunkDeletion& record);
            static TString Serialize(const TCompleteChunk& record);
        };

        class TDeleteRecordMerger {
            THashMap<ui8, ui64> OwnerToSeqNo;
            THashMap<TChunkSerNum, TDynBitMap> SerNumToChunk;

        public:
            struct TBlobDeletes {
                ui8 Owner;
                ui64 SeqNo;
                TVector<TBlobDeleteLocator> DeleteLocators;
            };

            // delete chunk -- a pure virtual record that is never written to real log
            struct TDeleteChunk {
                TChunkSerNum ChunkSerNum;
                ui32 NumItems;
            };

        public:
            void operator ()(const NKikimrVDiskData::TIncrHugeDelete& record);
            void operator ()(const TBlobDeletes& record);
            void operator ()(const TDeleteChunk& record);
            NKikimrVDiskData::TIncrHugeDelete GetCurrentState() const;

            static TString Serialize(const NKikimrVDiskData::TIncrHugeDelete& record);
            static TString Serialize(const TBlobDeletes& record);
            static TString Serialize(const TDeleteChunk& record);
        };

        class TLogger
            : public TKeeperComponentBase
        {
            // LSN of all chunks entrypoint
            ui64 ChunksEntrypointLsn = 0;

            // LSN of deletes entrypoint
            ui64 DeletesEntrypointLsn = 0;

            // LSN of next record
            ui64 Lsn = -1;

            // should we issue initial entrypoints?
            bool IssueChunksStartingPoint = false;

            // accumulated list of allocated chunks waiting for commit
            TVector<TChunkIdx> PendingCleanChunks;

            /////////////////////////////
            // Chunk record operations //
            /////////////////////////////

            struct TChunkQueueItem;
            using TChunkQueue = TList<TChunkQueueItem>;
            TChunkQueue ChunkQueue;
            TChunkQueue PendingChunkQueue;

            // confirmed state of chunk merger
            TChunkRecordMerger ConfirmedChunkMerger;

            bool LogChunksEntrypointPending = false;

            ui32 ProcessedChunksWithoutEntrypoint = 0;

            //////////////////////////////
            // Delete record operations //
            //////////////////////////////

            struct TDeleteQueueItem;
            using TDeleteQueue = TList<TDeleteQueueItem>;
            TDeleteQueue DeleteQueue;

            // this merger contains confirmed state of deletes -- that is it includes all successfully completed delete
            // log records
            TDeleteRecordMerger ConfirmedDeletesMerger;

            // number of process deletes
            ui32 ProcessedDeletesWithoutEntrypoint = 0;

#if INCRHUGE_HARDENED
            // sequence number check logic
            std::array<ui64, 256> LastSeqNo;
#endif

            ////////////////////////////////////
            // GetFirstLsnToKeep optimization //
            ////////////////////////////////////

            enum class EEntrypointType {
                None,
                Chunks,
                Deletes,
            };

        public:
            TLogger(TKeeper& keeper);
            ~TLogger();

            // update LSN with new state
            void SetLsnOnRecovery(ui64 lsn, bool issueInitialStartingPoints);

            ////////////
            // Chunks //
            ////////////

            void LogChunkAllocation(TVector<TChunkIdx>&& newChunkIds, TChunkSerNum baseSerNum,
                    std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx);

            // log deletion of chunk
            void LogChunkDeletion(TChunkIdx chunkIdx, TChunkSerNum chunkSerNum, ui32 numItems,
                    std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx);

            // log appearance of new filled chunk; this is auxiliary event and may be dropped under some conditions; if
            // it is not actually logged, then one on recovery should scan one more chunk instead of just reading index
            void LogCompleteChunk(TChunkIdx chunkIdx, TChunkSerNum chunkSerNum, const TActorContext& ctx);

            // log chunks entrypoint; this function should be called when there is need to update entrypoint -- by time,
            // by request of PDisk or by some other criterion; here we should mention that this function doesn't log
            // it immediately; instead of that is sets flag and that actual query is executed when all pending requests
            // are logged
            void LogChunksEntrypoint(const TActorContext& ctx);


            void SetInitialChunksState(const NKikimrVDiskData::TIncrHugeChunks& record);

            /////////////
            // Deletes //
            /////////////

            // log deletion of some blobs; requests are executed one by one in FIFO order; locators should come in
            // sorted order
            void LogBlobDeletes(ui8 owner, ui64 seqNo, TVector<TBlobDeleteLocator>&& deleteLocators,
                    std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx);

            void LogVirtualBlobDeletes(TVector<TBlobDeleteLocator>&& deleteLocators, const TActorContext& ctx);

            // set initial state
            void SetInitialDeletesState(const NKikimrVDiskData::TIncrHugeDelete& record);

            // generate deletes entrypoint
            void LogDeletesEntrypoint(const TActorContext& ctx);

            // calculate first LSN of actual log record
            ui64 GetFirstLsnToKeep(EEntrypointType ep) const;

            // handle cut log message from yard
            void HandleCutLog(NPDisk::TEvCutLog& msg, const TActorContext& ctx);

        private:
            /////////////////////////
            // Chunk queue helpers //
            /////////////////////////

            void ProcessChunkQueueItem(TChunkQueueItem&& newItem, const TActorContext& ctx);

            void ApplyLogChunkItem(TChunkQueueItem& item, NKikimrProto::EReplyStatus status, IEventBase *msg,
                    const TActorContext& ctx);

            void GenerateChunkEntrypoint(const TActorContext& ctx);

            //////////////////////////
            // Delete queue helpers //
            //////////////////////////

            // process new delete queue item and put it into delete queue
            void ProcessDeleteQueueItem(TDeleteQueueItem&& newItem, const TActorContext& ctx);

            void ProcessDeleteQueueVirtualItems(const TActorContext& ctx);

            // applies log result
            void ApplyLogDeleteItem(TDeleteQueueItem& item, NKikimrProto::EReplyStatus status, IEventBase *msg,
                    const TActorContext& ctx);
        };

        void DeserializeDeletes(TDynBitMap& deletedItems, const NKikimrVDiskData::TIncrHugeDelete::TChunkInfo& chunk);

    } // NIncrHuge
} // NKikimr
