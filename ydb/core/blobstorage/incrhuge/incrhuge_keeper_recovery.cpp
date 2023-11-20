#include "incrhuge_keeper_recovery.h"
#include "incrhuge_keeper_recovery_read_log.h"
#include "incrhuge_keeper_recovery_scan.h"
#include "incrhuge_keeper.h"

namespace NKikimr {
    namespace NIncrHuge {


        TRecovery::TRecovery(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Recovery")
        {}


        TRecovery::~TRecovery()
        {}

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDISK INIT LOGIC
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void TRecovery::HandleInit(TEvIncrHugeInit::TPtr& ev, const TActorContext& ctx) {
            TEvIncrHugeInit *msg = ev->Get();

            // initialize init queue item
            TInitQueueItem item{
                msg->VDiskId,  // VDiskId
                msg->Owner,    // Owner
                msg->FirstLsn, // FirstLsn
                ev->Sender,    // Sender
                ev->Cookie,    // Cookie
                {},            // ScanQueue
                {},            // DeletedItemsMap
                {}             // Items
            };

            // put it into the end of init queue and try to process it if we are ready
            InitQueue.push_back(std::move(item));
            if (Keeper.State.Ready) {
                ProcessInitItem(--InitQueue.end(), ctx);
            }
        }

        void TRecovery::ProcessInitItem(TInitQueue::iterator it, const TActorContext& ctx) {
            TInitQueueItem& item = *it;

            // make a snapshot of items being currently written
            item.Items = Keeper.Writer.EnumerateItems(item.Owner, item.FirstLsn);

            // make up a scan queue -- a queue consisting of complete chunks; all other chunks just have been enumerated
            for (auto& pair : Keeper.State.Chunks) {
                const TChunkIdx chunkIdx = pair.first;
                TChunkInfo& chunk = pair.second;
                if (chunk.State != EChunkState::Complete) {
                    continue;
                }

                // do not allow to delete this chunk while it is not scanned
                ++chunk.InFlightReq;

                // add chunk to scan queue
                item.ScanQueue.emplace(chunkIdx, &chunk);

                // create a snapshot of deleted items map for this chunk
                item.DeletedItemsMap.emplace(chunkIdx, chunk.DeletedItems);
            }

            // process scan queue as far as possible
            ProcessInitItemScanQueue(it, ctx);
        }

        void TRecovery::ProcessInitItemScanQueue(TInitQueue::iterator it, const TActorContext& ctx) {
            TInitQueueItem& item = *it;

            while (item.ScanQueue && (!ScanBytesInFlight || ScanBytesInFlight < Keeper.State.PDiskParams->ChunkSize)) {
                // extract first queue item
                TChunkIdx chunkIdx;
                TChunkInfo *chunk;
                std::tie(chunkIdx, chunk) = item.ScanQueue.front();
                item.ScanQueue.pop();

                // issue scan request
                const TActorId actorId = ctx.Register(CreateRecoveryScanActor(chunkIdx, true, chunk->ChunkSerNum,
                    static_cast<ui64>(EScanCookie::Recovery), Keeper.State));
                const bool added = Keeper.State.ChildActors.insert(actorId).second;
                Y_ABORT_UNLESS(added);

                // remember this actor
                ScannerMap.emplace(actorId, TScanInfo{it, chunkIdx, *chunk});

                // adjust in-flight counter
                ScanBytesInFlight += Keeper.State.BlocksInIndexSection * Keeper.State.BlockSize;
            }

            // check if we have finished all pending chunks
            if (!item.DeletedItemsMap) {
                SendInitResponse(it, NKikimrProto::OK, ctx);
            }
        }

        void TRecovery::ApplyInitItemScan(TInitQueue::iterator it, TChunkIdx chunkIdx, TChunkInfo& chunk,
                TEvIncrHugeScanResult& msg, const TActorContext& ctx) {
            TInitQueueItem& item = *it;

            // find deleted items snapshot for this chunk
            auto delIt = item.DeletedItemsMap.find(chunkIdx);
            Y_ABORT_UNLESS(delIt != item.DeletedItemsMap.end());
            TDynBitMap& deletedItems = delIt->second;

            // traverse index and extract required items
            for (ui32 i = 0; i < msg.Index.size(); ++i) {
                const TBlobIndexRecord& record = msg.Index[i];
                if (!deletedItems.Get(i) && record.Owner == item.Owner && record.Lsn >= item.FirstLsn) {
                    item.Items.push_back(TEvIncrHugeInitResult::TItem{
                            record.Id,
                            record.Lsn,
                            record.Meta
                        });
                }
            }

            // remove unused item map
            item.DeletedItemsMap.erase(delIt);

            // dereference chunk
            if (!--chunk.InFlightReq && chunk.State == EChunkState::Deleting) {
                Keeper.Deleter.IssueLogChunkDelete(chunkIdx, ctx);
            }

            // process more items
            ProcessInitItemScanQueue(it, ctx);
        }

        void TRecovery::SendInitResponse(TInitQueue::iterator it, NKikimrProto::EReplyStatus status,
                const TActorContext& ctx) {
            TInitQueueItem& item = *it;
            std::sort(item.Items.begin(), item.Items.end(), [](const auto& x, const auto& y) { return x.Lsn < y.Lsn; });
            ctx.Send(item.Sender, new TEvIncrHugeInitResult(status, std::move(item.Items)), 0, item.Cookie);
            InitQueue.erase(it);
        }

        void TRecovery::ApplyYardInit(NKikimrProto::EReplyStatus status, NPDisk::TLogRecord *chunks,
                NPDisk::TLogRecord *deletes, const TActorContext& ctx) {
            Y_ABORT_UNLESS(status == NKikimrProto::OK);

            static const TMaybe<ui64> none;
            const TActorId actorId = ctx.Register(CreateRecoveryReadLogActor(Keeper.State.Settings.PDiskActorId,
                    Keeper.State.PDiskParams->Owner, Keeper.State.PDiskParams->OwnerRound, chunks ? chunks->Lsn : none,
                    deletes ? deletes->Lsn : none));
            const bool added = Keeper.State.ChildActors.insert(actorId).second;
            Y_ABORT_UNLESS(added);
            IHLOG_INFO(ctx, "[IncrHugeKeeper PDisk# %09" PRIu32 "] starting ReadLog", Keeper.State.Settings.PDiskId);
        }

        void TRecovery::ApplyReadLog(const TActorId& sender, TEvIncrHugeReadLogResult& msg, const TActorContext& ctx) {
            const size_t num = Keeper.State.ChildActors.erase(sender);
            Y_ABORT_UNLESS(num == 1);

            Y_ABORT_UNLESS(msg.Status == NKikimrProto::OK);

            IHLOG_INFO(ctx, "[IncrHugeKeeper PDisk# %09" PRIu32 "] finished ReadLog", Keeper.State.Settings.PDiskId);

            TStringStream str;
            str << "Chunks# [";
            bool first = true;
            for (const auto& chunk : msg.Chunks.GetChunks()) {
                str << (first ? first = false, "" : " ");

                const char *state = "<unknown>";
                switch (chunk.GetState()) {
                    case NKikimrVDiskData::TIncrHugeChunks::Complete: state = "Complete"; break;
                    case NKikimrVDiskData::TIncrHugeChunks::WriteIntent: state = "WriteIntent"; break;
                    default: Y_ABORT("unexpected case");
                }

                TChunkSerNum chunkSerNum(chunk.GetChunkSerNum());
                str << Sprintf("{Chunk# %" PRIu32 "@%s State# %s}", chunk.GetChunkIdx(), chunkSerNum.ToString().data(),
                        state);
            }
            str << "] Deletes# [";
            first = true;
            for (const auto& chunk : msg.Deletes.GetChunks()) {
                str << (first ? first = false, "" : " ");
                str << Sprintf("{ChunkSerNum# %s Items# [", TChunkSerNum(chunk.GetChunkSerNum()).ToString().data());
                bool itemFirst = true;
                TDynBitMap deletedItems;
                DeserializeDeletes(deletedItems, chunk);
                Y_FOR_EACH_BIT(index, deletedItems) {
                    str << (itemFirst ? itemFirst = false, "" : " ") << index;
                }
                str << "]}";
            }
            str << "] Owners# {";
            first = true;
            for (const auto& owner : msg.Deletes.GetOwners()) {
                str << (first ? first = false, "" : " ") << owner.GetOwner() << ": " << owner.GetSeqNo();
            }
            str << Sprintf("} CurrentSerNum# %s NextLsn# %" PRIu64,
                    TChunkSerNum(msg.Chunks.GetCurrentSerNum()).ToString().data(), msg.NextLsn);
            IHLOG_DEBUG(ctx, "ApplyReadLog %s", str.Str().data());

            // apply LSN and set starting points flag
            Keeper.Logger.SetLsnOnRecovery(msg.NextLsn, msg.IssueInitialStartingPoints);

            // if the record has actual id, store it; otherwise previously there were no time to span id and
            // it has default value
            if (msg.Chunks.HasCurrentSerNum()) {
                Keeper.State.CurrentSerNum = TChunkSerNum(msg.Chunks.GetCurrentSerNum());
            }

            // parse filled chunks list
            TMap<TChunkSerNum, TScanQueueItem> scanQueue;
            THashMap<TChunkSerNum, TChunkInfo *> serNumToChunk;
            for (const auto& chunk : msg.Chunks.GetChunks()) {
                Y_ABORT_UNLESS(chunk.HasChunkIdx() && chunk.HasChunkSerNum() && chunk.HasState());

                EChunkState newState;
                bool indexOnly;

                switch (chunk.GetState()) {
                    case NKikimrVDiskData::TIncrHugeChunks::Complete:
                        newState = EChunkState::Complete;
                        indexOnly = true;
                        break;

                    case NKikimrVDiskData::TIncrHugeChunks::WriteIntent:
                        newState = EChunkState::Unknown;
                        indexOnly = false;
                        break;

                    default:
                        Y_ABORT("unexpected case");
                }

                const TChunkSerNum chunkSerNum(chunk.GetChunkSerNum());

                TChunkInfo chunkInfo{
                    newState,    // State
                    chunkSerNum, // ChunkSerNum
                    0,           // NumUsedBlocks
                    0,           // NumItems
                    {},          // DeletedItems
                    0,           // InFlightReq
                };

                auto chunkIt = Keeper.State.Chunks.emplace(chunk.GetChunkIdx(), std::move(chunkInfo)).first;
                serNumToChunk.emplace(chunkSerNum, &chunkIt->second);

                // enqueue scan; they are executed in order of increasing serial number, so we store them in sorted
                // map
                scanQueue.emplace(chunkSerNum, TScanQueueItem{chunk.GetChunkIdx(), indexOnly, chunkSerNum});
            }

            // start scans
            for (auto& pair : scanQueue) {
                EnqueueScan(std::move(pair.second), ctx);
                ChunkSerNumQueue.push(pair.first);
            }
            decltype(scanQueue)().swap(scanQueue);

            Keeper.Logger.SetInitialChunksState(msg.Chunks);

            // apply deletes
            for (const auto& chunk : msg.Deletes.GetChunks()) {
                // extract chunk's serial number
                Y_ABORT_UNLESS(chunk.HasChunkSerNum());
                const TChunkSerNum chunkSerNum(chunk.GetChunkSerNum());

                // find chunk with such serial number
                auto it = serNumToChunk.find(chunkSerNum);
                if (it == serNumToChunk.end()) {
                    // obsolete record -- ignoring it
                    continue;
                }

                // deserialize deletes into found chunk
                DeserializeDeletes(it->second->DeletedItems, chunk);
            }
            Keeper.Logger.SetInitialDeletesState(msg.Deletes);

            // apply deletes' owners
            for (const auto& owner : msg.Deletes.GetOwners()) {
                Y_ABORT_UNLESS(owner.HasOwner() && owner.HasSeqNo());
                Keeper.Deleter.InsertOwnerOnRecovery(owner.GetOwner(), owner.GetSeqNo());
            }

            ProcessScanQueue(ctx);
        }

        void TRecovery::ApplyScan(const TActorId& sender, TEvIncrHugeScanResult& msg, const TActorContext& ctx) {
            const size_t num = Keeper.State.ChildActors.erase(sender);
            Y_ABORT_UNLESS(num == 1);

            const ui32 bytes = msg.IndexOnly ? Keeper.State.BlocksInIndexSection * Keeper.State.BlockSize :
                Keeper.State.PDiskParams->ChunkSize;
            Y_ABORT_UNLESS(ScanBytesInFlight >= bytes);
            ScanBytesInFlight -= bytes;

            // try to find actor in scanner map
            auto it = ScannerMap.find(sender);
            if (it != ScannerMap.end()) {
                TScanInfo& scan = it->second;
                ApplyInitItemScan(scan.It, scan.ChunkIdx, scan.Chunk, msg, ctx);
                ScannerMap.erase(it);
                return;
            }

            // extract valuable information from message
            TScanResult scanResult{
                msg.Status,
                msg.ChunkIdx,
                msg.IndexOnly,
                msg.IndexValid,
                std::move(msg.Index)
            };

            // ensure that we still have chunk serial numbers to process; they are stored in ascending order, so we
            // process chunks starting from the oldest one
            Y_ABORT_UNLESS(ChunkSerNumQueue);

            // if we can process this message right now, do it; otherwise store it for deferred execution
            if (msg.ChunkSerNum == ChunkSerNumQueue.front()) {
                ChunkSerNumQueue.pop();
                ProcessScanResult(scanResult, ctx);

                // we have advanced in current serial number, so check for deferred items queue -- we may have some
                // responses there
                decltype(PendingResults)::iterator it;
                while (ChunkSerNumQueue && (it = PendingResults.find(ChunkSerNumQueue.front())) != PendingResults.end()) {
                    ChunkSerNumQueue.pop();
                    ProcessScanResult(it->second, ctx);
                    PendingResults.erase(it);
                }

                // issue more requests if possible
                ProcessScanQueue(ctx);
            } else {
                // just save the message for further processing
                PendingResults.emplace(msg.ChunkSerNum, std::move(scanResult));
            }
        }

        void TRecovery::ProcessScanResult(TScanResult& scanResult, const TActorContext& ctx) {
            // find matching chunk
            TChunkInfo& chunk = Keeper.State.Chunks.at(scanResult.ChunkIdx);

            IHLOG_DEBUG(ctx, "ProcessScanResult ChunkIdx# %" PRIu32 " ChunkSerNum# %s IndexOnly# %s IndexValid# %s",
                    scanResult.ChunkIdx, chunk.ChunkSerNum.ToString().data(), scanResult.IndexOnly ? "true" : "false",
                    scanResult.IndexValid ? "true" : "false");

            if (scanResult.Status == NKikimrProto::NODATA) {
                // this is really empty chunk, put it to write intent queue
                Y_ABORT_UNLESS(!scanResult.IndexOnly);
                chunk.State = EChunkState::WriteIntent;
                Keeper.State.WriteIntentQueue.push(scanResult.ChunkIdx);
            } else if (scanResult.Status == NKikimrProto::OK) {
                // chunk with some data; calculate number of used blocks
                ui32 numUsedBlocks = 0;
                ui32 offsetInBlocks = 0;
                TVector<TBlobDeleteLocator> deleteLocators;
                for (ui32 index = 0; index < scanResult.Index.size(); ++index) {
                    const TBlobIndexRecord& record = scanResult.Index[index];

                    // calculate item size in blocks
                    const ui32 sizeInBlocks = (sizeof(TBlobHeader) + record.PayloadSize + Keeper.State.BlockSize - 1) /
                        Keeper.State.BlockSize;

//                    Cerr << Sprintf("ChunkSerNum# %s index# %" PRIu32 " record# %s deleted# %s\n",
//                            ~chunk.ChunkSerNum.ToString(), index, ~record.ToString(),
//                            chunk.DeletedItems.Get(index) ? "true" : "false");

                    // count as used blocks unless this item is deleted
                    TBlobLocator existing;
                    if (!chunk.DeletedItems.Get(index)) {
                        numUsedBlocks += sizeInBlocks;

                        // create locator for new item
                        TBlobLocator locator{scanResult.ChunkIdx, offsetInBlocks, record.PayloadSize, ui32(index),
                            record.Owner, false};

                        // try to insert new item; allow duplicates to be returned
                        if (TBlobLocator *existing = Keeper.State.BlobLookup.Insert(record.Id, TBlobLocator(locator), true)) {
                            // find existing chunk; it should exist just because we have created it recently during
                            // recovery
                            auto existingIt = Keeper.State.Chunks.find(existing->ChunkIdx);
                            Y_ABORT_UNLESS(existingIt != Keeper.State.Chunks.end());
                            TChunkInfo& existingChunk = existingIt->second;

                            // create delete locator for one of records (existing or new one)
                            TBlobDeleteLocator deleteLocator;

                            // sort by chunk ids; chunk ids are generated in ascending order, so newer chunks will have
                            // greater id that the old ones; the only possible situation when duplicates occur is when
                            // defragmented blob was not yet deleted from its previous location, so record with the most
                            // id will win this race and will be marked as the real location of blob; the older record
                            // will be marked as deleted
                            if (existingChunk.ChunkSerNum < chunk.ChunkSerNum) {
                                // existing record is older than the new one -- that it a duplicate and should be removed
                                deleteLocator = {existing->ChunkIdx, existingChunk.ChunkSerNum, {},
                                    existing->IndexInsideChunk, sizeInBlocks};
                                // replace existing record content with new one
                                *existing = locator;
                            } else if (chunk.ChunkSerNum < existingChunk.ChunkSerNum) {
                                // fail as is breaks our order of processing -- old chunks first, then new chunks
                                Y_ABORT("unexpected order of chunks");
                            } else {
                                // chunks are the same; this could happen if defragmenter was processing the current
                                // chunk we were writing into, but this is completely incorrect behavior
                                Y_ABORT("duplicate records; Old# %s New# %s ChunkSerNum# %s index# %" PRIu32,
                                        existing->ToString().data(), locator.ToString().data(),
                                        existingChunk.ChunkSerNum.ToString().data(), index);
                            }

                            // schedule this record for deletion; do not delete it right now as it can be occasionally
                            // deletes
                            deleteLocators.push_back(std::move(deleteLocator));
                        }
                    } else if (Keeper.State.BlobLookup.Delete(record.Id, &existing)) {
                        // we found existing locator, but newer record has delete flag set -- it means that this blob
                        // was found in defragmented chunk and should be deleted
                        auto it = Keeper.State.Chunks.find(existing.ChunkIdx);
                        Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                        TChunkInfo& chunk = it->second;
                        deleteLocators.push_back(TBlobDeleteLocator{existing.ChunkIdx, chunk.ChunkSerNum, {},
                                existing.IndexInsideChunk, sizeInBlocks});
                    }

                    // advance offset iterator
                    offsetInBlocks += sizeInBlocks;
                }

                // update number of used blocks
                chunk.NumUsedBlocks = numUsedBlocks;
                chunk.NumItems = scanResult.Index.size();

                // make actual deletes
                Keeper.Deleter.DeleteDefrag(std::move(deleteLocators), ctx);

                // check if index for this record is correct; if so, then put it into complete state
                if (scanResult.IndexValid) {
                    chunk.State = EChunkState::Complete;
                } else {
                    IncompleteChunks.emplace(chunk.ChunkSerNum, TIncompleteChunk{scanResult.ChunkIdx, offsetInBlocks,
                            std::move(scanResult.Index)});
                }
            } else {
                Y_ABORT("can't recover");
            }
        }

        void TRecovery::EnqueueScan(TScanQueueItem&& item, const TActorContext& ctx) {
            if (ScanQueue || !ProcessScanItem(item, ctx)) {
                ScanQueue.push(std::move(item));
            }
        }

        bool TRecovery::ProcessScanItem(TScanQueueItem& item, const TActorContext& ctx) {
            const ui32 bytes = item.IndexOnly ? Keeper.State.BlocksInIndexSection * Keeper.State.BlockSize :
                Keeper.State.PDiskParams->ChunkSize;
            if (ScanBytesInFlight && ScanBytesInFlight + bytes > Keeper.State.PDiskParams->ChunkSize) {
                return false;
            }
            const TActorId actorId = ctx.Register(CreateRecoveryScanActor(item.ChunkIdx, item.IndexOnly,
                    item.ChunkSerNum, static_cast<ui64>(EScanCookie::Recovery), Keeper.State));
            const bool added = Keeper.State.ChildActors.insert(actorId).second;
            Y_ABORT_UNLESS(added);
            IHLOG_DEBUG(ctx, "[IncrHugeKeeper PDisk# %09" PRIu32 "] scan ChunkIdx# %" PRIu32
                    " IndexOnly# %s", Keeper.State.Settings.PDiskId, item.ChunkIdx, item.IndexOnly ? "true" : "false");
            ScanBytesInFlight += bytes;
            return true;
        }

        void TRecovery::ProcessScanQueue(const TActorContext& ctx) {
            while (ScanQueue && ProcessScanItem(ScanQueue.front(), ctx)) {
                ScanQueue.pop();
            }
            if (!ScanQueue && !ScanBytesInFlight) {
                // check that we have actually processed all the scans and there are no hanging results
                Y_ABORT_UNLESS(!ChunkSerNumQueue && !PendingResults);

                // apply incomplete chunks in order of write intent queue
                for (auto& pair : IncompleteChunks) {
                    TIncompleteChunk& ic = pair.second;

                    // check if there is a current chunk already set, then set up writer to write out index
                    if (Keeper.State.CurrentChunk) {
                        Keeper.Writer.FinishCurrentChunk(ctx);
                        Y_ABORT_UNLESS(!Keeper.State.CurrentChunk);
                    }

                    // set up new current chunk
                    TChunkInfo& chunk = Keeper.State.Chunks.at(ic.ChunkIdx);
                    chunk.State = EChunkState::Current;
                    Keeper.State.CurrentChunk = ic.ChunkIdx;
                    Keeper.Writer.SetUpCurrentChunk(ic.OffsetInBlocks, std::move(ic.Index));
                }
                IncompleteChunks.clear();

                // set ready flag and start processing all pending init queries
                Keeper.State.Ready = true;
                auto it = InitQueue.begin();
                while (it != InitQueue.end()) {
                    auto next = std::next(it);
                    ProcessInitItem(it, ctx);
                    it = next;
                }

                // push all chunks to defragmenter as we are ready now
                for (const auto& pair : Keeper.State.Chunks) {
                    Keeper.Defragmenter.UpdateChunkState(pair.first, ctx);
                }

                Keeper.Allocator.CheckForAllocationNeed(ctx);

                IHLOG_INFO(ctx, "[IncrHugeKeeper PDisk# %09" PRIu32 "] ready", Keeper.State.Settings.PDiskId);
            }
        }

    } // NIncrHuge
} // NKikimr
