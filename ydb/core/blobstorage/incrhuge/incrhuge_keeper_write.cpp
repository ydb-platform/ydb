#include "incrhuge_keeper_write.h"
#include "incrhuge_keeper.h"
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
    namespace NIncrHuge {

        TWriter::TWriter(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Writer")
        {}

        TWriter::~TWriter()
        {}

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // WRITE QUEUE LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TWriter::TWriteQueueItem {
            ////////////////////////
            // Request parameters //
            ////////////////////////

            // query unique id
            ui64 QueryId;

            // owner who sent this request
            ui8 Owner;

            // LSN in owner's space; just a number we don't examine here
            ui64 Lsn;

            // blob metadata with we also wouldn't examine
            TBlobMetadata Meta;

            // data to write
            TString Data;

            // event payload which will be moved to response
            TWritePayloadPtr Payload;

            // the actor who sent this request
            TActorId Sender;

            // cookie of request; the same for response
            ui64 Cookie;

            // callback which will be invoked upon completion
            std::unique_ptr<IEventCallback> Callback;

            ////////////////////
            // Executor state //
            ////////////////////

            // are header and id fields valid?
            bool Defrag;

            // blob header -- we store it here until it is written becase there is a pointer to it in parts and we own
            // this data
            TBlobHeader Header;

            // list of parts we write into chunk; up to three parts -- mandatory both header and data, and optional
            // padding (if total size of data and header isn't multiple of block size)
            std::array<NPDisk::TEvChunkWrite::TPart, 3> Parts;

            // blob id
            TIncrHugeBlobId Id;

            // the chunk we store this blob into
            TChunkIdx ChunkIdx;

            // item's size in blocks
            ui32 SizeInBlocks;

            // item index inside chunk
            ui32 IndexInsideChunk;

            // item offset inside chunk
            ui32 OffsetInBlocks;

            // original item chunk index
            TChunkIdx OriginalChunkIdx;

            // original item chunk serial number
            TChunkSerNum OriginalChunkSerNum;

            // original item's index within its chunk
            ui32 OriginalIndexInsideChunk;
        };

        // write request handler; it does nothing but formatting new item into TWriteQueueItem and kicking queue processor
        void TWriter::HandleWrite(TEvIncrHugeWrite::TPtr& ev, const TActorContext& ctx) {
            // extract pointer to event
            TEvIncrHugeWrite *msg = ev->Get();

            // create an item to process
            TWriteQueueItem item{
                NextQueryId++,           // QueryId
                msg->Owner,              // Owner
                msg->Lsn,                // Lsn
                msg->Meta,               // Meta
                std::move(msg->Data),    // Data
                std::move(msg->Payload), // Payload
                ev->Sender,              // Sender
                ev->Cookie,              // Cookie
                {},                      // Callback (will be filled in later)
                false,                   // Defrag
                {},                      // Header
                {},                      // Parts
                {},                      // Id
                {},                      // ChunkIdx
                {},                      // SizeInBlocks
                {},                      // IndexInsideChunk
                {},                      // OffsetInBlocks
                {},                      // OriginalChunkIdx
                {},                      // OriginalChunkSerNum
                {}};                     // OriginalIndexInsideChunk

            // put item into queue
            auto it = WriteQueue.insert(WriteQueue.end(), std::move(item));

            // fill in callback
            auto callback = [it](NKikimrProto::EReplyStatus status, IEventBase* /*msg*/, const TActorContext& ctx) {
                ctx.Send(it->Sender, new TEvIncrHugeWriteResult(status, it->Id, std::move(it->Payload)), 0, it->Cookie);
            };
            it->Callback = MakeCallback(std::move(callback));

            IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " HandleWrite Lsn# %" PRIu64 " DataSize# %" PRIu32
                    " WriteQueueSize# %zu WriteInProgressItemsSize# %zu", it->QueryId, it->Lsn, ui32(it->Data.size()),
                    WriteQueue.size(), WriteInProgressItems.size());

            // kick processor
            ProcessWriteQueue(ctx);
        }

        // defragmentation write handler; puts an item into write queue and kick executor
        void TWriter::EnqueueDefragWrite(const TBlobHeader& header, TChunkIdx chunkIdx, TChunkSerNum chunkSerNum,
                ui32 indexInsideChunk, TString&& data, std::unique_ptr<IEventCallback>&& callback, const TActorContext& ctx) {
            // create a queue item
            TWriteQueueItem item{
                NextQueryId++,           // QueryId
                {},                      // Owner
                {},                      // Lsn
                {},                      // Meta
                std::move(data),         // Data
                {},                      // Payload
                {},                      // Sender
                {},                      // Cookie
                std::move(callback),     // Callback
                true,                    // Defrag
                header,                  // Header
                {},                      // Parts
                header.IndexRecord.Id,   // Id
                {},                      // ChunkIdx
                {},                      // SizeInBlocks
                {},                      // IndexInsideChunk
                {},                      // OffsetInBlocks
                chunkIdx,                // OriginalChunkIdx
                chunkSerNum,             // OriginalChunkSerNum
                indexInsideChunk};       // OriginalIndexInsideChunk

            // put it in queue
            WriteQueue.push_back(std::move(item));

            // kick processor
            ProcessWriteQueue(ctx);
        }

        // queue processor; this function tries to process as much items in WriteQueue in FIFO order as possible; it stops
        // when either queue ends, or there is not enough space to fit current blob in current chunk and there is no
        // other chunk in write intent queue, or in flight counter exceeded its maximum value
        void TWriter::ProcessWriteQueue(const TActorContext& ctx) {
            IHLOG_DEBUG(ctx, "WriteQueueSize# %zu WriteInProgressItemsSize# %zu",
                    WriteQueue.size(), WriteInProgressItems.size());
            for (auto it = WriteQueue.begin(); it != WriteQueue.end() &&
                    WriteInProgressItems.size() < Keeper.State.Settings.MaxInFlightWrites; ) {
                // iterator is postincremented because it may be invalidated inside this function
                if (!ProcessWriteItem(it++, ctx)) {
                    break;
                }
            }
        }

        // queue item processor; it is called for WriteQueue items and, if possible, starts to write blob into chunk;
        // if there is not enough space in current blob, then current chunk is 'finalized' -- that is, the index is
        // formatted and written to chunk and next chunk is started; next chunk is always taken from the write intent
        // queue; writing consists of preparing data (like filling header fields and so on) and sending request to yard
        // to write to the chunk; if the request is sent, then item is moved from WriteQueue to WriteInProgressItems
        bool TWriter::ProcessWriteItem(TWriteQueue::iterator it, const TActorContext& ctx) {
            TWriteQueueItem& item = *it;

            IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ProcessWriteItem entry", item.QueryId);

            // for items coming from defragmenter we should first check if this item was deleted while it was in write
            // queue; for such items we return special condition
            if (item.Defrag && IsObsolete(item, ctx)) {
                item.Callback->Apply(NKikimrProto::RACE, nullptr, ctx);
                IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ProcessWriteItem defrag, obsolete", item.QueryId);
                WriteQueue.erase(it);
                return true;
            }

            // if this is item from user and there is not enough disk space to process query, abort this request
            if (!item.Defrag && Keeper.State.DiskState <= EDiskState::SpaceOrange) {
                item.Callback->Apply(NKikimrProto::OUT_OF_SPACE, nullptr, ctx);
                IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ProcessWriteItem disk space", item.QueryId);
                WriteQueue.erase(it);
                return true;
            }

            // calculate payload size in bytes and ensure it fits into required boundaries
            const ui32 payloadSize = item.Data.size();
            Y_ABORT_UNLESS(payloadSize >= Keeper.State.Settings.MinHugeBlobInBytes && payloadSize < 0x1000000);

            // calculate full data record size in blocks; round total size of header and payload up to a block size
            const ui32 sizeInBlocks = (sizeof(TBlobHeader) + payloadSize + Keeper.State.BlockSize - 1) / Keeper.State.BlockSize;

            // if we have current chunk and there is no space to fit this item, we have to finish this chunk; inside
            // callee function current chunk state will be reset to 'no current chunk'
            if (Keeper.State.CurrentChunk && CurrentChunkOffsetInBlocks + sizeInBlocks > Keeper.State.BlocksInDataSection) {
                FinishCurrentChunk(ctx);
                Y_DEBUG_ABORT_UNLESS(!Keeper.State.CurrentChunk);
            }

            // if we have no current chunk, then try to allocate one; this usually happens if we just have finished
            // our last chunk, but also may happen on start
            if (!Keeper.State.CurrentChunk && !TryToBeginNewChunk(ctx)) {
                // can't handle this now, no chunks to write into, because write intent queue is empty; this function
                // should be called once again when allocation occurs
                IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ProcessWriteItem no free chunks", item.QueryId);
                return false;
            }

            // ensure we have a chunk to write into
            Y_ABORT_UNLESS(Keeper.State.CurrentChunk);
            TChunkInfo& chunk = Keeper.State.Chunks.at(Keeper.State.CurrentChunk);
            Y_ABORT_UNLESS(chunk.State == EChunkState::Current);

            // store chunk we are writing to into queue item
            item.ChunkIdx = Keeper.State.CurrentChunk;

            // if this request came from user, we have to generate blob's id and fill in its header; for writes from
            // defragmenter these fields are already set
            TBlobHeader& header = item.Header;
            if (!item.Defrag) {
                // generate id, register it in blob lookup table
                item.Id = Keeper.State.BlobLookup.Create(TBlobLocator{Keeper.State.CurrentChunk,
                        CurrentChunkOffsetInBlocks, payloadSize, chunk.NumItems, item.Owner, false});

                memset(&header, 0, sizeof(header));

                // fill in index record which is a part of blob header
                TBlobIndexRecord& indexRecord = header.IndexRecord;
                indexRecord.Id = item.Id;
                indexRecord.Lsn = item.Lsn;
                indexRecord.Meta = item.Meta;
                indexRecord.PayloadSize = payloadSize;
                indexRecord.Owner = item.Owner;
            }

            // fill in header; calculate checksum for header (excluding CRC) + data
            header.ChunkSerNum = chunk.ChunkSerNum;
            header.Checksum = Crc32cExtend(Crc32c(&header.IndexRecord, sizeof(TBlobHeader) - sizeof(ui32)),
                    item.Data.data(), item.Data.size());
            static_assert(offsetof(TBlobHeader, Checksum) == 0 && sizeof(TBlobHeader::Checksum) == sizeof(ui32),
                    "incorrect displacement of TBlobHeader::Checksum");

            item.SizeInBlocks = sizeInBlocks;
            item.IndexInsideChunk = chunk.NumItems;
            item.OffsetInBlocks = CurrentChunkOffsetInBlocks;

            // calculate padding required to get round request
            const ui32 padding = sizeInBlocks * Keeper.State.BlockSize - (sizeof(TBlobHeader) + payloadSize);

            // fill in parts array and create parts object to send it into yard; there are three parts -- blob header,
            // then blob data and padding to fill up to block size; if padding is zero, it is not added
            ui32 numParts = 0;
            item.Parts[numParts++] = {&item.Header, sizeof(TBlobHeader)};
            item.Parts[numParts++] = {item.Data.data(), payloadSize};
            if (padding) {
                item.Parts[numParts++] = {nullptr, padding};
            }

            // calculate total size
            ui32 totalSize = 0;
            for (ui32 i = 0; i < numParts; ++i) {
                totalSize += item.Parts[i].Size;
            }

            // move this item into execution queue
            WriteInProgressItems.splice(WriteInProgressItems.end(), WriteQueue, it);
            ++Keeper.State.InFlightWrites;

            // create callback lambda that will be invoked upon write completion; this lambda invokes post-write handler
            // inside this class and then removes item from write-in-progress set and kicks queue processor to possibly
            // use just freed slot
            auto callback = [this, it](NKikimrProto::EReplyStatus status, IEventBase *result, const TActorContext& ctx) {
                ApplyBlobWrite(status, *it, result, ctx);
                WriteInProgressItems.erase(it);
                --Keeper.State.InFlightWrites;
                ProcessWriteQueue(ctx);
                Keeper.Defragmenter.InFlightWritesChanged(ctx);
            };

            // prepare and send write message to yard
            const ui32 offset = CurrentChunkOffsetInBlocks * Keeper.State.BlockSize;
            auto writeMsg = std::make_unique<NPDisk::TEvChunkWrite>(Keeper.State.PDiskParams->Owner,
                    Keeper.State.PDiskParams->OwnerRound, item.ChunkIdx, offset,
                    new NPDisk::TEvChunkWrite::TNonOwningParts(item.Parts.data(), numParts), Keeper.RegisterYardCallback(
                    MakeCallback(std::move(callback))), true, NPriWrite::HullHugeUserData, true);
            ctx.Send(Keeper.State.Settings.PDiskActorId, writeMsg.release());
            ++CurrentChunkWritesInFlight;

            IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ProcessWriteItem OffsetInBlocks# %" PRIu32 " IndexInsideChunk# %"
                    PRIu32 " SizeInBlocks# %" PRIu32 " SizeInBytes# %" PRIu32 " Offset# %" PRIu32 " Size# %" PRIu32
                    " End# %" PRIu32 " Id# %016" PRIx64 " ChunkIdx# %" PRIu32 " ChunkSerNum# %s Defrag# %s",
                    item.QueryId, CurrentChunkOffsetInBlocks, chunk.NumItems, sizeInBlocks, sizeInBlocks *
                    Keeper.State.BlockSize, offset, totalSize, offset + totalSize, item.Id, item.ChunkIdx,
                    chunk.ChunkSerNum.ToString().data(), item.Defrag ? "true" : "false");

            // if this is defragmentation item, then put it into special hash map indicating that it is 'write in progress'
            // in this case, if delete request comes for such item, it should wait for defragmentation to finish
            if (item.Defrag) {
                Keeper.State.DefragWriteInProgress.emplace(item.Id, false);
            }

            // shift write pointers and update number of used blocks in current chunk; it may differ with offset
            // if blobs in current chunk were deleted while filling in the chunk
            CurrentChunkOffsetInBlocks += sizeInBlocks;
            chunk.NumUsedBlocks += sizeInBlocks;
            ++chunk.NumItems;

            // add index record
            CurrentChunkIndex.push_back(item.Header.IndexRecord);

            return true;
        }

        // blob write postprocessor code; it generates response to client and then checks if this chunk is in finalization
        // state
        void TWriter::ApplyBlobWrite(NKikimrProto::EReplyStatus status, TWriteQueueItem& item, IEventBase *result,
                const TActorContext& ctx) {
            IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " ApplyBlobWrite Status# %s", item.QueryId,
                    NKikimrProto::EReplyStatus_Name(status).data());

            Y_ABORT_UNLESS(status == NKikimrProto::OK, "don't know how to handle errors yet");

            // check if this item is from defragmenter
            if (item.Defrag) {
                // update item locator for a new place
                Keeper.State.BlobLookup.Replace(item.Id, TBlobLocator{item.ChunkIdx, item.OffsetInBlocks,
                        item.Header.IndexRecord.PayloadSize, item.IndexInsideChunk, item.Header.IndexRecord.Owner,
                        false});

                // remove item from set and kick deleter to resume operation for this item (if any)
                auto it = Keeper.State.DefragWriteInProgress.find(item.Id);
                Y_ABORT_UNLESS(it != Keeper.State.DefragWriteInProgress.end());
                if (it->second) {
                    Keeper.Deleter.OnItemDefragWritten(item.Id, ctx);
                }
                Keeper.State.DefragWriteInProgress.erase(it);
            }

            // invoke callback
            item.Callback->Apply(status, result, ctx);

            if (item.ChunkIdx == Keeper.State.CurrentChunk) {
                Y_ABORT_UNLESS(CurrentChunkWritesInFlight > 0);
                --CurrentChunkWritesInFlight;
            } else {
                auto it = FinalizingChunks.find(item.ChunkIdx);
                Y_ABORT_UNLESS(it != FinalizingChunks.end());
                TFinalizingChunk& fin = it->second;
                Y_ABORT_UNLESS(fin.WritesInFlight > 0);
                --fin.WritesInFlight;
                if (CheckFinalizingChunk(item.ChunkIdx, fin, ctx)) {
                    FinalizingChunks.erase(it);
                }
            }
        }

        bool TWriter::IsObsolete(const TWriteQueueItem& item, const TActorContext& ctx) {
            bool obsolete = true;

            // try to find chunk this item was in; if there is no such chunk, then item is obsolete
            auto it = Keeper.State.Chunks.find(item.OriginalChunkIdx);
            if (it != Keeper.State.Chunks.end()) {
                // ensure this chunk has the same version as recorded one
                TChunkInfo& chunk = it->second;
                if (chunk.ChunkSerNum == item.OriginalChunkSerNum) {
                    // check for deletion
                    obsolete = chunk.DeletedItems.Get(item.OriginalIndexInsideChunk);
                    if (!obsolete) {
                        // find locator
                        const TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(item.Header.IndexRecord.Id);
                        Y_ABORT_UNLESS(locator.ChunkIdx == item.OriginalChunkIdx);
                        obsolete = locator.DeleteInProgress;
                        IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " DeleteInProgress# %s", item.QueryId,
                                obsolete ? "true" : "false");
                    } else {
                        IHLOG_DEBUG(ctx, "QueryId# %" PRIu64 " obsolete# true", item.QueryId);
                    }
                }
            }

            return obsolete;
        }

        void TWriter::SetUpCurrentChunk(ui32 offsetInBlocks, TVector<TBlobIndexRecord>&& index) {
            CurrentChunkOffsetInBlocks = offsetInBlocks;
            CurrentChunkIndex = std::move(index);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CHUNK ALLOCATION LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bool TWriter::TryToBeginNewChunk(const TActorContext& ctx) {
            // see if we have some items in intent queue; if it is empty, return false meaning that we can't satisfy
            // this request right now
            if (Keeper.State.WriteIntentQueue.empty()) {
                return false;
            }

            // pop intent queue item
            TChunkIdx chunkIdx = Keeper.State.WriteIntentQueue.front();
            Keeper.State.WriteIntentQueue.pop();

            // find matching item in chunk set and set it as current
            auto it = Keeper.State.Chunks.find(chunkIdx);
            Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
            TChunkInfo& chunk = it->second;
            Y_ABORT_UNLESS(chunk.State == EChunkState::WriteIntent);
            chunk.State = EChunkState::Current;

            // store current chunk index in common state
            Keeper.State.CurrentChunk = chunkIdx;

            // initialize local writer state
            CurrentChunkOffsetInBlocks = 0;
            CurrentChunkIndex.clear();
            CurrentChunkWritesInFlight = 0;

            // kick allocator because we have used one of write intent chunks; it may ask to allocate more of them
            Keeper.Allocator.CheckForAllocationNeed(ctx);

            return true;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // CHUNK FINALIZATION LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TWriter::TIndexWriteQueueItem {
            // the chunk we are finalizing
            TChunkIdx ChunkIdx;
            // index header
            TBlobIndexHeader Header;
            // list of parts to write; up to four: data padding, header, index, index padding
            std::array<NPDisk::TEvChunkWrite::TPart, 4> Parts;
        };

        void TWriter::FinishCurrentChunk(const TActorContext& ctx) {
            // create finalizing chunk record; store metadata here, it may be required in deletion and enumeration
            // this record lives longer that TIndexWriteQueueItem, so we store chunk index here
            Y_DEBUG_ABORT_UNLESS(!FinalizingChunks.count(Keeper.State.CurrentChunk));
            TFinalizingChunk fin;
            fin.WritesInFlight = CurrentChunkWritesInFlight;
            fin.Index = std::move(CurrentChunkIndex);
            auto finIt = FinalizingChunks.emplace(Keeper.State.CurrentChunk, std::move(fin)).first;

            // create new index write queue item
            auto it = IndexWriteQueue.emplace(IndexWriteQueue.end());
            TIndexWriteQueueItem& item = *it;

            // ensure we have current chunk we are writing into and store it into item
            Y_ABORT_UNLESS(Keeper.State.CurrentChunk);
            item.ChunkIdx = Keeper.State.CurrentChunk;

            // find current chunk and update its state
            TChunkInfo& chunk = Keeper.State.Chunks.at(Keeper.State.CurrentChunk);
            Y_ABORT_UNLESS(chunk.State == EChunkState::Current);
            chunk.State = EChunkState::Finalizing;

            // calculate alignment up to data section end
            const ui32 dataPadding = (Keeper.State.BlocksInDataSection - CurrentChunkOffsetInBlocks) * Keeper.State.BlockSize;

            // generate index header
            TBlobIndexHeader& indexHeader = item.Header;
            memset(&indexHeader, 0, sizeof(indexHeader));
            indexHeader.ChunkSerNum = chunk.ChunkSerNum;
            indexHeader.NumItems = finIt->second.Index.size();
            indexHeader.Checksum = Crc32cExtend(Crc32c(&indexHeader.ChunkSerNum,
                    sizeof(indexHeader) - sizeof(TBlobIndexHeader::Checksum)),
                    finIt->second.Index.data(), indexHeader.NumItems * sizeof(TBlobIndexRecord));
            static_assert(offsetof(TBlobIndexHeader, Checksum) == 0 && sizeof(TBlobIndexHeader::Checksum) == sizeof(ui32),
                    "incorrect displacement of TBlobIndexHeader::Checksum");

            // move index to queue item and calculate its size
            const ui32 indexSize = indexHeader.NumItems * sizeof(TBlobIndexRecord);

            // calculate index padding
            ui32 indexPadding = Keeper.State.BlocksInIndexSection * Keeper.State.BlockSize - (sizeof(TBlobIndexHeader) +
                    indexSize);

            // calculate parts we have to write
            ui32 numParts = 0;
            if (dataPadding) {
                item.Parts[numParts++] = {nullptr, dataPadding};
            }
            item.Parts[numParts++] = {&indexHeader, sizeof(TBlobIndexHeader)};
            item.Parts[numParts++] = {finIt->second.Index.data(), indexSize};
            if (indexPadding) {
                item.Parts[numParts++] = {nullptr, indexPadding};
            }
            ui32 totalSize = 0;
            for (ui32 i = 0; i < numParts; ++i) {
                totalSize += item.Parts[i].Size;
            }
            Y_ABORT_UNLESS(totalSize % Keeper.State.PDiskParams->AppendBlockSize == 0, "totalSize# %" PRIu32
                    " AppendBlockSize# %" PRIu32, totalSize, Keeper.State.PDiskParams->AppendBlockSize);

            // calculate and validate offset
            const ui32 offset = CurrentChunkOffsetInBlocks * Keeper.State.BlockSize;
            Y_ABORT_UNLESS(offset + totalSize <= Keeper.State.PDiskParams->ChunkSize);

            // create callback object
            auto callback = [this, it](NKikimrProto::EReplyStatus status, IEventBase* /*result*/, const TActorContext& ctx) {
                ApplyIndexWrite(status, *it, ctx);
                IndexWriteQueue.erase(it);
            };

            IHLOG_DEBUG(ctx, "IndexWrite chunkIdx# %" PRIu32 " offset# %" PRIu32 " size# %"
                    PRIu32 " end# %" PRIu32, item.ChunkIdx, offset, totalSize, offset + totalSize);

            // send write message to yard
            ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvChunkWrite(Keeper.State.PDiskParams->Owner,
                    Keeper.State.PDiskParams->OwnerRound, item.ChunkIdx, offset,
                    new NPDisk::TEvChunkWrite::TNonOwningParts(item.Parts.data(), numParts),
                    Keeper.RegisterYardCallback(MakeCallback(std::move(callback))), true, NPriWrite::HullHugeUserData,
                    true));

            // clear current chunk state
            Keeper.State.CurrentChunk = 0;
        }

        void TWriter::ApplyIndexWrite(NKikimrProto::EReplyStatus status, TIndexWriteQueueItem& item, const TActorContext& ctx) {
            Y_ABORT_UNLESS(status == NKikimrProto::OK, "don't know how to handle errors yet");

            auto it = FinalizingChunks.find(item.ChunkIdx);
            Y_ABORT_UNLESS(it != FinalizingChunks.end());
            TFinalizingChunk& fin = it->second;
            Y_ABORT_UNLESS(!fin.IndexWritten);
            fin.IndexWritten = true;
            if (CheckFinalizingChunk(item.ChunkIdx, fin, ctx)) {
                FinalizingChunks.erase(it);
            }
        }

        bool TWriter::CheckFinalizingChunk(ui32 chunkIdx, TFinalizingChunk& fin, const TActorContext& ctx) {
            if (fin.WritesInFlight == 0 && fin.IndexWritten) {
                TChunkInfo& chunk = Keeper.State.Chunks.at(chunkIdx);
                Y_ABORT_UNLESS(chunk.State == EChunkState::Finalizing);

                // check if all chunk contents were deleted while finalizing; if so, this record should be deleted
                if (!chunk.NumUsedBlocks) {
                    IHLOG_DEBUG(ctx, "deleting ChunkIdx# %" PRIu32, chunkIdx);
                    Keeper.Deleter.IssueLogChunkDelete(chunkIdx, ctx);
                    return true;
                }

                // ensure the chunk is not empty
                Y_ABORT_UNLESS(chunk.NumUsedBlocks > 0);

                // set state to complete
                chunk.State = EChunkState::Complete;

                // he have just filled in new chunk, so we may log it; we do not block until this request completes
                // because maximum side-effect of failure is a little recovery performance impact due to need of
                // extra chunk scan
                Keeper.Logger.LogCompleteChunk(chunkIdx, chunk.ChunkSerNum, ctx);

                // update defragmenter state for this chunk
                Keeper.Defragmenter.UpdateChunkState(chunkIdx, ctx);

                return true;
            }

            return false;
        }

        TVector<TEvIncrHugeInitResult::TItem> TWriter::EnumerateItems(ui8 owner, ui64 firstLsn) {
            TVector<TEvIncrHugeInitResult::TItem> res;
            auto scanChunk = [&](TChunkIdx chunkIdx, const TVector<TBlobIndexRecord>& index) {
                auto it = Keeper.State.Chunks.find(chunkIdx);
                Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                TChunkInfo& chunk = it->second;
                for (ui32 i = 0; i < index.size(); ++i) {
                    const TBlobIndexRecord& record = index[i];
                    if (!chunk.DeletedItems.Get(i) && record.Owner == owner && record.Lsn >= firstLsn) {
                        res.push_back(TEvIncrHugeInitResult::TItem{
                                record.Id,
                                record.Lsn,
                                record.Meta
                            });
                    }
                }
            };

            if (const TChunkIdx chunkIdx = Keeper.State.CurrentChunk) {
                scanChunk(chunkIdx, CurrentChunkIndex);
            }
            for (const auto& pair : FinalizingChunks) {
                scanChunk(pair.first, pair.second.Index);
            }

            return res;
        }

    } // NIncrHuge
} // NKikimr
