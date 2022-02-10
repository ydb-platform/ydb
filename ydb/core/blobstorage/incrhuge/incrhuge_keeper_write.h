#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge.h"
#include "incrhuge_data.h"

#include <util/generic/queue.h>

namespace NKikimr {
    namespace NIncrHuge {

        // keeper forward reference
        class TKeeper;

        class TWriter
            : public TKeeperComponentBase
        {
            // offset (in blocks) inside current chunks
            ui32 CurrentChunkOffsetInBlocks = 0;

            // index of current chunk
            TVector<TBlobIndexRecord> CurrentChunkIndex;

            // number of in-flight blob write requests to current chunk
            ui32 CurrentChunkWritesInFlight = 0;

            // write queue
            struct TWriteQueueItem;
            using TWriteQueue = TList<TWriteQueueItem>;
            TWriteQueue WriteQueue; // pending items
            TWriteQueue WriteInProgressItems; // items in work; they all are executed concurrently and may finish out-of-order

            // index write queue
            struct TIndexWriteQueueItem;
            using TIndexWriteQueue = TList<TIndexWriteQueueItem>;
            TIndexWriteQueue IndexWriteQueue;

            // finalization chunk queue
            struct TFinalizingChunk {
                // number of writes in flight to this chunk
                ui32 WritesInFlight = 0;

                // index write finished?
                bool IndexWritten = false;

                // index of chunk being finalized
                TVector<TBlobIndexRecord> Index;
            };

            THashMap<TChunkIdx, TFinalizingChunk> FinalizingChunks;

            ui64 NextQueryId = 0;

        public:
            TWriter(TKeeper& keeper);
            ~TWriter();

            //////////////////////////
            // write queue handling //
            //////////////////////////

            // write request handler
            void HandleWrite(TEvIncrHugeWrite::TPtr& ev, const TActorContext& ctx);

            // defragmentation write handler
            void EnqueueDefragWrite(const TBlobHeader& header, TChunkIdx chunkIdx, TChunkSerNum chunkSerNum,
                    ui32 indexInsideChunk, TString&& data, std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx);

            // process queue items as much as possible; this function should be called when conditions to process queue
            // items successfully may occur; for example, chunk allocation occured or one of previous writes finished
            void ProcessWriteQueue(const TActorContext& ctx);

            // try to process item inside write queue; it may start processing if there is enough space, then item
            // will be marked as in progress; if item processing is started, returns true; otherwise false
            bool ProcessWriteItem(TWriteQueue::iterator it, const TActorContext& ctx);

            // blob writer callback; called when write operation finishes
            void ApplyBlobWrite(NKikimrProto::EReplyStatus status, TWriteQueueItem& item, IEventBase *result,
                    const TActorContext& ctx);

            // reset writer to specified position (used in recovery)
            void SetUpCurrentChunk(ui32 offsetInBlocks, TVector<TBlobIndexRecord>&& index);

            // check if item is obsolete
            bool IsObsolete(const TWriteQueueItem& item, const TActorContext& ctx);

            //////////////////////
            // Chunk allocation //
            //////////////////////

            bool TryToBeginNewChunk(const TActorContext& ctx);

            ////////////////////////
            // Chunk finalization //
            ////////////////////////

            // this function generates index and issues write message to PDisk
            void FinishCurrentChunk(const TActorContext& ctx);

            // this callback is called when index write is finished
            void ApplyIndexWrite(NKikimrProto::EReplyStatus status, TIndexWriteQueueItem& item, const TActorContext& ctx);

            // check if chunk is completely finalized, that is index is written and there is no blob writes in flight,
            // and then put this chunk into filled chunks map; in this case function return true; otherwise it returns
            // false
            bool CheckFinalizingChunk(TChunkIdx chunkIdx, TFinalizingChunk& fin, const TActorContext& ctx);

            // get pointer to chunk in 'finalizing' state; this is needed by deleter to clean up items in such chunks
            TFinalizingChunk *GetFinalizingChunk(TChunkIdx chunkIdx);

            // put finalizing chunks to log record
            void PutFinalizingChunks(NKikimrVDiskData::TIncrHugeChunks *record) const;

            /////////////////
            // Client init //
            /////////////////

            TVector<TEvIncrHugeInitResult::TItem> EnumerateItems(ui8 owner, ui64 firstLsn);
        };

    } // NIncrHuge
} // NKikimr
