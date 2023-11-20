#include "incrhuge_keeper_delete.h"
#include "incrhuge_data.h"
#include "incrhuge_keeper_log.h"
#include "incrhuge_keeper.h"
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {
    namespace NIncrHuge {

        enum class EItemState {
            WaitingForDefrag, // item is waiting for defragmenter to finish its job for involved items
            Ready,            // item is ready to be sent to logger
            Processing,       // item is waiting for logger to process request
        };

        struct TDeleter::TDeleteQueueItem {
            const TActorId Sender;
            const ui64 Cookie;
            const ui8 Owner;
            const ui64 SeqNo;
            const TVector<TIncrHugeBlobId> Ids;

            ui32 NumDefragItems; // number of items being defragmented right now
            TVector<TBlobDeleteLocator> DeleteLocators;
            EItemState State;
        };

        TDeleter::TDeleter(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Deleter")
        {
            memset(OwnerToSeqNo.data(), 0, OwnerToSeqNo.size() * sizeof(ui64));
        }

        TDeleter::~TDeleter()
        {}

        void TDeleter::HandleDelete(TEvIncrHugeDelete::TPtr& ev, const TActorContext& ctx) {
            // get a message plain pointer
            TEvIncrHugeDelete *msg = ev->Get();

            // generate message into text log
            auto makeIdList = [&] {
                TStringStream s;
                for (size_t i = 0; i < msg->Ids.size(); ++i) {
                    s << (i == 0 ? "" : " ") << Sprintf("%016" PRIx64, msg->Ids[i]);
                }
                return s.Str();
            };
            IHLOG_DEBUG(ctx, "Owner# %d SeqNo# %" PRIu64 " HandleDelete Ids# [%s]", msg->Owner, msg->SeqNo,
                    makeIdList().data());

            // verify sequence number -- it should exceed maximum value of stored number and all requests in-flight or
            // else this is duplicate query
            Y_ABORT_UNLESS(msg->Owner < OwnerToSeqNo.size());
            ui64 ownerSeqNo = OwnerToSeqNo[msg->Owner];
            if (DeleteQueue) {
                const ui64 inFlightSeqNo = DeleteQueue.back().SeqNo;
                Y_ABORT_UNLESS(inFlightSeqNo > ownerSeqNo, "inFlightSeqNo# %" PRIu64 " ownerSeqNo# %" PRIu64,
                        inFlightSeqNo, ownerSeqNo);
                ownerSeqNo = inFlightSeqNo;
            }
            if (msg->SeqNo <= ownerSeqNo) {
                ctx.Send(ev->Sender, new TEvIncrHugeDeleteResult(NKikimrProto::ALREADY), 0, ev->Cookie);
                return;
            }

            // create delete queue item
            auto it = DeleteQueue.insert(DeleteQueue.end(), TDeleteQueueItem{
                    ev->Sender,          // Sender
                    ev->Cookie,          // Cookie
                    msg->Owner,          // Owner
                    msg->SeqNo,          // SeqNo
                    std::move(msg->Ids), // Ids
                    0,                   // NumDefragItems
                    {},                  // DeleteLocators
                    EItemState::Ready,   // State
                });
            TDeleteQueueItem& item = *it;

            // create delete locators for specified items
            for (TIncrHugeBlobId id : item.Ids) {
                // find locator for item, it must exist
                TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(id);

                // mark it as being deleted; ensure that there are no other deletion races
                Y_ABORT_UNLESS(!locator.DeleteInProgress);
                locator.DeleteInProgress = true;

                // check if item is being defragmented right now; if so, we put it into wait queue; otherwise generate
                // delete locator
                auto defragIt = Keeper.State.DefragWriteInProgress.find(id);
                if (defragIt != Keeper.State.DefragWriteInProgress.end()) {
                    Y_ABORT_UNLESS(!defragIt->second);
                    defragIt->second = true;
                    ++item.NumDefragItems;
                    Y_ABORT_UNLESS(!WriteInProgress.count(id));
                    WriteInProgress.emplace(id, it);
                    item.State = EItemState::WaitingForDefrag;
                    IHLOG_DEBUG(ctx, "Owner# %d SeqNo# %" PRIu64 " Id# %016" PRIx64 " deferred delete",
                            item.Owner, item.SeqNo, id);
                } else {
                    // find matching chunk, check that it is not being deleted
                    auto it = Keeper.State.Chunks.find(locator.ChunkIdx);
                    Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                    TChunkInfo& chunk = it->second;
                    Y_ABORT_UNLESS(chunk.State != EChunkState::Deleting);

                    // check that requested item is not deleted yet
                    Y_ABORT_UNLESS(!chunk.DeletedItems.Get(locator.IndexInsideChunk));

                    // calculate number of blocks occupied by this blob
                    const ui32 sizeInBlocks = Keeper.State.GetBlobSizeInBlocks(locator.PayloadSize);

                    // create delete locator entry
                    item.DeleteLocators.push_back(TBlobDeleteLocator{locator.ChunkIdx, chunk.ChunkSerNum, id,
                            locator.IndexInsideChunk, sizeInBlocks});
                }
            }

            // try to process this item
            ProcessDeleteItem(it, ctx);
        }

        // this function is invoked as a notification for item that was moved when delete query for it was received
        void TDeleter::OnItemDefragWritten(TIncrHugeBlobId id, const TActorContext& ctx) {
            auto it = WriteInProgress.find(id);
            Y_ABORT_UNLESS(it != WriteInProgress.end());
            TDeleteQueue::iterator itemIt = it->second;
            WriteInProgress.erase(it);
            TDeleteQueueItem& item = *itemIt;

            // obtain new locator for this item
            const TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(id);

            // get chunk
            auto chunkIt = Keeper.State.Chunks.find(locator.ChunkIdx);
            Y_ABORT_UNLESS(chunkIt != Keeper.State.Chunks.end());
            TChunkInfo& chunk = chunkIt->second;

            // calculate size of this record in blocks
            const ui32 sizeInBlocks = Keeper.State.GetBlobSizeInBlocks(locator.PayloadSize);

            // create delete locator entry
            item.DeleteLocators.push_back(TBlobDeleteLocator{locator.ChunkIdx, chunk.ChunkSerNum, id,
                    locator.IndexInsideChunk, sizeInBlocks});

            Y_ABORT_UNLESS(item.NumDefragItems > 0);
            if (!--item.NumDefragItems) {
                IHLOG_DEBUG(ctx, "Owner# %d SeqNo# %" PRIu64 " delete resumed", item.Owner, item.SeqNo);
                item.State = EItemState::Ready;
                ProcessDeleteItem(itemIt, ctx);

                // we have just started processing of one of the items; may be we can kick waiting items after that too?
                for (auto it = std::next(itemIt); it != DeleteQueue.end() && it->State == EItemState::Ready; ++it) {
                    ProcessDeleteItem(it, ctx);
                }
            }
        }

        // this function is called for every item in delete queue, whether is was generated by user request; it may be
        // called lately after receiving request if one (or more) of items scheduled for deletion were being moved
        // right now; in this case deleter waits for all item moves to finish and actually executes user query
        void TDeleter::ProcessDeleteItem(TDeleteQueue::iterator it, const TActorContext& ctx) {
            TDeleteQueueItem& item = *it;

            // if previous item doesn't have 'Processing' state, then do not process this item too -- to keep ordering
            if (it != DeleteQueue.begin() && std::prev(it)->State != EItemState::Processing) {
                return;
            }
            switch (item.State) {
                case EItemState::WaitingForDefrag:
                    // we can't process this item yet
                    Y_ABORT_UNLESS(item.NumDefragItems > 0);
                    return;

                case EItemState::Ready:
                    // switch to processing this item
                    item.State = EItemState::Processing;
                    break;

                case EItemState::Processing:
                    Y_ABORT("unexpected case");
            }

            // ensure that we generated locator for each item
            Y_ABORT_UNLESS(item.DeleteLocators.size() == item.Ids.size());

            // sort locators as needed
            std::sort(item.DeleteLocators.begin(), item.DeleteLocators.end());

            // create callback to be invoked upon successful (or unsuccessful) logging of delete record
            auto callback = [this, it](NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
                TDeleteQueueItem& item = *it;

                IHLOG_DEBUG(ctx, "Owner# %d SeqNo# %" PRIu64 " finished Status# %s",
                        item.Owner, item.SeqNo, NKikimrProto::EReplyStatus_Name(status).data());

                if (status == NKikimrProto::OK) {
                    // handle deleted locators; remove them from lookup also
                    ProcessDeletedLocators(item.DeleteLocators, true, ctx);
                    // update per-owner sequential number; we do this only in case of success
                    Y_ABORT_UNLESS(item.Owner < OwnerToSeqNo.size());
                    ui64& ownerSeqNo = OwnerToSeqNo[item.Owner];
                    Y_ABORT_UNLESS(item.SeqNo > ownerSeqNo);
                    ownerSeqNo = item.SeqNo;
                } else {
                    // if delete fails, then reset deletion flag for scheduled items
                    for (TIncrHugeBlobId id : item.Ids) {
                        // find locator
                        TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(id);
                        // ensure delete flag was set and reset it
                        Y_ABORT_UNLESS(locator.DeleteInProgress);
                        locator.DeleteInProgress = false;
                    }
                }

                // send reply to sender
                ctx.Send(item.Sender, new TEvIncrHugeDeleteResult(status), 0, item.Cookie);

                // delete queue item
                DeleteQueue.erase(it);
            };

            // send request to logger
            Keeper.Logger.LogBlobDeletes(item.Owner, item.SeqNo, TVector<TBlobDeleteLocator>(item.DeleteLocators),
                    MakeSimpleCallback(std::move(callback)), ctx);
        }

        // this function is called for items that were defragmented; a vector of delete locators contains metadata for
        // these items, but TIncrHugeBlobId's are zero to BadId as these items are not deleted from index -- they were
        // just moved
        void TDeleter::DeleteDefrag(TVector<TBlobDeleteLocator>&& deleteLocators, const TActorContext& ctx) {
            // sort items
            std::sort(deleteLocators.begin(), deleteLocators.end());
            // issue virtual log record to keep internal and log state consistent if delete chunk message is generated
            Keeper.Logger.LogVirtualBlobDeletes(TVector<TBlobDeleteLocator>(deleteLocators), ctx);
            // execute actual deletion; this must be called after logging virtual delete record, because this function
            // may issue chunk deletions and they must be consistent with deletions -- chunks may only be dropped after
            // _ALL_ their blobs are marked deleted
            ProcessDeletedLocators(deleteLocators, false, ctx);
        }

        // this function actually applies delete operations; it deletes items from index (if requested) and marks items
        // in chunks as deleted ones; this should be called only after successfully logging changes
        void TDeleter::ProcessDeletedLocators(const TVector<TBlobDeleteLocator>& deleteLocators, bool deleteFromLookup,
                const TActorContext& ctx) {
            if (deleteFromLookup) {
                for (const TBlobDeleteLocator& deleteLocator : deleteLocators) {
                    IHLOG_DEBUG(ctx, "deleting %016" PRIx64 " from lookup table", deleteLocator.Id);
                    TBlobLocator locator;
                    bool status = Keeper.State.BlobLookup.Delete(deleteLocator.Id, &locator);
                    Y_ABORT_UNLESS(status);
                }
            }

            // clear chunks
            for (auto it = deleteLocators.begin(); it != deleteLocators.end(); ) {
                // get reference to current chunk
                const TChunkIdx chunkIdx = it->ChunkIdx;
                auto chunkIt = Keeper.State.Chunks.find(chunkIdx);
                Y_ABORT_UNLESS(chunkIt != Keeper.State.Chunks.end());
                TChunkInfo& chunk = chunkIt->second;
                Y_ABORT_UNLESS(chunk.State != EChunkState::Deleting);

                // process items
                for (; it != deleteLocators.end() && it->ChunkIdx == chunkIdx; ++it) {
                    Y_ABORT_UNLESS(chunk.ChunkSerNum == it->ChunkSerNum);
                    Y_ABORT_UNLESS(chunk.NumUsedBlocks >= it->SizeInBlocks);
                    chunk.NumUsedBlocks -= it->SizeInBlocks;
                    Y_ABORT_UNLESS(!chunk.DeletedItems.Get(it->IndexInsideChunk));
                    chunk.DeletedItems.Set(it->IndexInsideChunk);
                }

                // for complete chunks see if we can release this chunk; otherwise we mark deleted items there
                if (chunk.State == EChunkState::Complete && !chunk.NumUsedBlocks) {
                    IssueLogChunkDelete(chunkIdx, ctx);
                }

                // notify defragmenter
                Keeper.Defragmenter.UpdateChunkState(chunkIdx, ctx);
            }
        }

        // this function is called for a chunk to transfer it to Deleting state and generate log entry with chunk
        // deletion; chunk is actually removed from index only when log operation succeeds; if log fails, then it is
        // retries infinitely until success; chunk is not deleted if there are requests in flight for this chunk, what
        // is indicated by non-zero value of chunk.InFlightReq; every time when this counter reaches zero as a result
        // of decrement and chunk is in 'Deleting' state, one should invoke this function
        void TDeleter::IssueLogChunkDelete(TChunkIdx chunkIdx, const TActorContext& ctx) {
            // update chunk state to deleting
            TChunkInfo& chunk = Keeper.State.Chunks.at(chunkIdx);
            chunk.State = EChunkState::Deleting;

            // if there are requests in flight, do nothing yet
            if (chunk.InFlightReq) {
                return;
            }

            // ensure that chunk is completely deleted, i.e. all of its items are marked deleted; mostly this is debug
            // feature
            Y_ABORT_UNLESS(chunk.NumUsedBlocks == 0 && chunk.DeletedItems.Count() == chunk.NumItems);
            for (ui32 i = 0; i < chunk.NumItems; ++i) {
                Y_ABORT_UNLESS(chunk.DeletedItems.Get(i));
            }

            // create callback that will actually delete this chunk from index when log is completed
            auto callback = [this, chunkIdx](NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
                IHLOG_DEBUG(ctx, "finished chunk delete ChunkIdx# %" PRIu32 " Status# %s", chunkIdx,
                        NKikimrProto::EReplyStatus_Name(status).data());

                // find chunk and ensure that it is in deleting state
                auto it = Keeper.State.Chunks.find(chunkIdx);
                Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                TChunkInfo& chunk = it->second;
                Y_ABORT_UNLESS(chunk.State == EChunkState::Deleting);
                Y_ABORT_UNLESS(chunk.InFlightReq == 0);

                // on success just delete chunk from set; otherwise try again
                if (status == NKikimrProto::OK) {
                    Keeper.State.Chunks.erase(it);
                } else {
                    IssueLogChunkDelete(chunkIdx, ctx);
                }
            };

            IHLOG_DEBUG(ctx, "sending chunk delete ChunkIdx# %" PRIu32, chunkIdx);

            // log chunk deletion; generate "DeletedChunks" item for this record in order to remove this chunk from
            // chunks set on recovery
            Keeper.Logger.LogChunkDeletion(chunkIdx, chunk.ChunkSerNum, chunk.NumItems,
                    MakeSimpleCallback(std::move(callback)), ctx);
        }

        // this function is called during recovery to set up initial positions of sequence number for each owner
        void TDeleter::InsertOwnerOnRecovery(ui8 owner, ui64 seqNo) {
            Y_ABORT_UNLESS(owner < OwnerToSeqNo.size());
            OwnerToSeqNo[owner] = seqNo;
        }

    } // NIncrHuge
} // NKikimr
