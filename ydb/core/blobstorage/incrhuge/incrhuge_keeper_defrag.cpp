#include "incrhuge_keeper_defrag.h"
#include "incrhuge_keeper.h"
#include "incrhuge_keeper_recovery_scan.h"
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
    namespace NIncrHuge {

        TDefragmenter::TDefragmenter(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Defragmenter")
        {
            Threshold = 0.8;
        }

        TDefragmenter::~TDefragmenter()
        {}

        void TDefragmenter::HandleControlDefrag(TEvIncrHugeControlDefrag::TPtr& ev, const TActorContext& ctx) {
            Threshold = ev->Get()->Threshold;
            for (const auto& pair : Keeper.State.Chunks) {
                UpdateChunkState(pair.first, ctx);
            }
        }

        void TDefragmenter::UpdateChunkState(TChunkIdx /*chunkIdx*/, const TActorContext& ctx) {
            ProcessPendingChunksQueue(ctx);
        }

        void TDefragmenter::InFlightWritesChanged(const TActorContext& ctx) {
            ProcessPendingChunksQueue(ctx);
        }

        void TDefragmenter::ProcessPendingChunksQueue(const TActorContext& ctx) {
            // do not process more than 1 chunk at once
            if (ChunkInProgress || InFlightReads || InFlightWrites || !Keeper.State.Ready || Keeper.State.InFlightWrites > 3) {
                return;
            }

            // calculate total efficiency
            ui64 numUsedBlocks = 0;
            for (const auto& pair : Keeper.State.Chunks) {
                const TChunkInfo& chunk = pair.second;
                numUsedBlocks += chunk.NumUsedBlocks;
            }
            double efficiency = (double)numUsedBlocks / (Keeper.State.Chunks.size() * Keeper.State.BlocksInDataSection);
            IHLOG_DEBUG(ctx, "overall efficiency %.3lf", efficiency);
            if (efficiency >= Threshold) {
                return;
            }

            // find worst chunk
            double worstEfficiency = 1.0;
            for (const auto& pair : Keeper.State.Chunks) {
                const TChunkIdx chunkIdx = pair.first;
                const TChunkInfo& chunk = pair.second;

                if (chunk.State != EChunkState::Complete) {
                    // ignore other kinds of chunks, they're not ready for defragmentation yet
                    continue;
                }

                // calculate efficiency and choose the worst one
                const double efficiency = (double)chunk.NumUsedBlocks / Keeper.State.BlocksInDataSection;
                if (efficiency < worstEfficiency || !ChunkInProgress) {
                    ChunkInProgress = chunkIdx;
                    ChunkInProgressSerNum = chunk.ChunkSerNum;
                    worstEfficiency = efficiency;
                }
            }

            // if there were no chunk selected, exit
            if (!ChunkInProgress) {
                return;
            }

            // notify
            IHLOG_DEBUG(ctx, "starting ChunkInProgress# %" PRIu32 " efficiency# %.3lf", ChunkInProgress, worstEfficiency);

            // create chunk scanner to read index
            const TActorId actorId = ctx.Register(CreateRecoveryScanActor(ChunkInProgress, true, ChunkInProgressSerNum,
                static_cast<ui64>(EScanCookie::Defrag), Keeper.State), TMailboxType::HTSwap, AppData(ctx)->BatchPoolId);
            const bool inserted = Keeper.State.ChildActors.insert(actorId).second;
            Y_ABORT_UNLESS(inserted);
        }

        TChunkInfo *TDefragmenter::CheckCurrentChunk(const TActorContext& ctx) {
            Y_ABORT_UNLESS(ChunkInProgress);
            auto it = Keeper.State.Chunks.find(ChunkInProgress);
            if (it == Keeper.State.Chunks.end() || it->second.ChunkSerNum != ChunkInProgressSerNum ||
                    it->second.State == EChunkState::Deleting) {
                // chunk is either not found nor has version mismatch -- this means that our original chunk we were
                // working with is gone now
                FinishChunkInProgress(ctx);
                return nullptr;
            } else {
                Y_ABORT_UNLESS(it->second.State == EChunkState::Complete);
                return &it->second;
            }
        }

        void TDefragmenter::ApplyScan(const TActorId& sender, TEvIncrHugeScanResult& msg, const TActorContext& ctx) {
            const size_t num = Keeper.State.ChildActors.erase(sender);
            Y_ABORT_UNLESS(num == 1);

            IHLOG_DEBUG(ctx, "ApplyScan received");

            if (!CheckCurrentChunk(ctx)) {
                // if this chunk is finished, then we have nothing to do with this reply, simply ignore it
                return;
            }

            Y_ABORT_UNLESS(msg.Status == NKikimrProto::OK);

            // generate some messages to read blobs
            Index = std::move(msg.Index);
            IndexPos = 0;
            OffsetInBlocks = 0;
            ProcessIndex(ctx);
        }

        void TDefragmenter::ProcessIndex(const TActorContext& ctx) {
            TChunkInfo *chunkPtr = CheckCurrentChunk(ctx);
            if (!chunkPtr) {
                return;
            }
            TChunkInfo& chunk = *chunkPtr;

            // process current index until there are records to process
            while (IndexPos < Index.size() && InFlightReads < MaxInFlightReads && InFlightReadBytes < MaxInFlightReadBytes
                    && InFlightWrites < MaxInFlightWrites) {
                // get record header
                const TBlobIndexRecord& record = Index[IndexPos];

                // calculate number of blocks this blob occupies
                const ui32 sizeInBlocks = Keeper.State.GetBlobSizeInBlocks(record.PayloadSize);

                // check if this record is not deleted
                if (!chunk.DeletedItems.Get(IndexPos)) {
                    // find locator for this blob and check if is still points here
                    const TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(record.Id);
                    Y_ABORT_UNLESS(locator.ChunkIdx == ChunkInProgress);
                    Y_ABORT_UNLESS(locator.OffsetInBlocks == OffsetInBlocks);
                    Y_ABORT_UNLESS(locator.PayloadSize == record.PayloadSize);
                    Y_ABORT_UNLESS(locator.IndexInsideChunk == IndexPos);
                    Y_ABORT_UNLESS(locator.Owner == record.Owner);

                    // create read callback
                    auto callback = [this, record, chunkIdx = ChunkInProgress, chunkSerNum = ChunkInProgressSerNum,
                                offsetInBlocks = OffsetInBlocks, index = IndexPos]
                            (NKikimrProto::EReplyStatus status, IEventBase *msg, const TActorContext& ctx) {
                        ApplyRead(record, chunkIdx, chunkSerNum, offsetInBlocks, index, status,
                                *static_cast<NPDisk::TEvChunkReadResult*>(msg), ctx);
                    };

                    // issue read
                    const ui32 offset = OffsetInBlocks * Keeper.State.BlockSize;
                    const ui32 size = sizeInBlocks * Keeper.State.BlockSize;
                    ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvChunkRead(Keeper.State.PDiskParams->Owner,
                            Keeper.State.PDiskParams->OwnerRound, ChunkInProgress, offset, size, NPriRead::HullComp,
                            Keeper.RegisterYardCallback(MakeCallback(std::move(callback)))));

                    IHLOG_DEBUG(ctx, "sending TEvChunkRead ChunkIdx# %" PRIu32 " OffsetInBlocks# %" PRIu32
                            " sizeInBlocks# %" PRIu32, ChunkInProgress, OffsetInBlocks, sizeInBlocks);

                    ++InFlightReads;
                    InFlightReadBytes += size;

                    ++chunk.InFlightReq;
                    Y_ABORT_UNLESS(chunk.State != EChunkState::Deleting);
                }

                // advance index and update offset in blocks
                ++IndexPos;
                OffsetInBlocks += sizeInBlocks;
            }

            // if current chunks ends and there are no more reads in flight, time to switch to another chunk
            if (IndexPos == Index.size() && !InFlightReads && !InFlightWrites) {
                Y_ABORT_UNLESS(chunk.State == EChunkState::Complete);
                FinishChunkInProgress(ctx);
            }
        }

        void TDefragmenter::ApplyRead(const TBlobIndexRecord& record, TChunkIdx chunkIdx, TChunkSerNum chunkSerNum,
                ui32 offsetInBlocks, ui32 index, NKikimrProto::EReplyStatus status, NPDisk::TEvChunkReadResult& result,
                const TActorContext& ctx) {
            const ui32 sizeInBlocks = Keeper.State.GetBlobSizeInBlocks(record.PayloadSize);
            const ui32 totalSize = sizeof(TBlobHeader) + record.PayloadSize;

            IHLOG_DEBUG(ctx, "ApplyRead offsetInBlocks# %" PRIu32 " index# %" PRIu32 " Status# %s", offsetInBlocks,
                    index, NKikimrProto::EReplyStatus_Name(status).data());

            // adjust number of in-flight requests
            const ui32 bytes = sizeInBlocks * Keeper.State.BlockSize;
            Y_ABORT_UNLESS(InFlightReads > 0 && InFlightReadBytes >= bytes);
            --InFlightReads;
            InFlightReadBytes -= bytes;

            // remove in-flight request; we may be blocking chunk deletion here (are there were read request to chunk
            // scheduled for deletion); if so and this was the last request, try to delete this chunk again
            auto it = Keeper.State.Chunks.find(chunkIdx);
            Y_ABORT_UNLESS(it != Keeper.State.Chunks.end() && it->second.ChunkSerNum == chunkSerNum);
            --it->second.InFlightReq;
            if (!it->second.InFlightReq && it->second.State == EChunkState::Deleting) {
                Keeper.Deleter.IssueLogChunkDelete(chunkIdx, ctx);
            }

            // ignore this answer if this is not our current chunk
            if (chunkIdx != ChunkInProgress || chunkSerNum != ChunkInProgressSerNum) {
                return;
            }

            // check if current chunk still intact
            TChunkInfo *chunkPtr = CheckCurrentChunk(ctx);
            if (!chunkPtr) {
                return;
            }

            // get a refernce to our chunk
            TChunkInfo& chunk = *chunkPtr;

            // check if item wasn't deleted while read was executing
            if (status == NKikimrProto::OK && !chunk.DeletedItems.Get(index) && result.Data.IsReadable(0, sizeof(TBlobHeader))) {
                // lookup blob locator for this item
                const TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(record.Id);

                // validate this locator
                Y_ABORT_UNLESS(locator.ChunkIdx == ChunkInProgress && locator.OffsetInBlocks == offsetInBlocks
                        && locator.PayloadSize == record.PayloadSize && locator.IndexInsideChunk == index
                        && locator.Owner == record.Owner, "locator# %s offsetInBlocks# %" PRIu32 " index# %" PRIu32
                        " record# %s", locator.ToString().data(), offsetInBlocks, index, record.ToString().data());

                const TBlobHeader& header = *result.Data.DataPtr<const TBlobHeader>(0);
                if (!locator.DeleteInProgress && result.Data.IsReadable(0, totalSize)) {
                    Y_ABORT_UNLESS(Crc32c(&header.IndexRecord, totalSize - sizeof(ui32)) == header.Checksum);
                    Y_ABORT_UNLESS(header.IndexRecord == record);
                    Y_ABORT_UNLESS(header.ChunkSerNum == ChunkInProgressSerNum);
                    TString data = header.ExtractInplacePayload();

                    // fill in delete locator with provided values
                    TBlobDeleteLocator deleteLocator{chunkIdx, chunkSerNum, {}, index, sizeInBlocks};

                    // create callback which will be invoked when write is completed
                    auto callback = [this, deleteLocator](NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
                        ApplyWrite(deleteLocator, status, ctx);
                    };

                    // enqueue writer queue item; it should check if item was deleted when starting actual execution
                    // of queue item and after writing it
                    IHLOG_DEBUG(ctx, "EnqueueDefragWrite chunkIdx# %" PRIu32 " index# %" PRIu32 " Id# %016" PRIx64,
                            chunkIdx, index, header.IndexRecord.Id);
                    Keeper.Writer.EnqueueDefragWrite(header, chunkIdx, chunk.ChunkSerNum, index, std::move(data),
                            MakeSimpleCallback(std::move(callback)), ctx);
                    ++InFlightWrites;
                }
            }

            // generate some more read requests if it is possible
            ProcessIndex(ctx);
        }

        void TDefragmenter::ApplyWrite(const TBlobDeleteLocator& deleteLocator, NKikimrProto::EReplyStatus status,
                const TActorContext& ctx) {
            // RACE is returned when the original item was deleted while write was in waiting in queue or in progress
            if (status != NKikimrProto::RACE) {
                // ensure that write succeeds FIXME: error handling
                Y_ABORT_UNLESS(status == NKikimrProto::OK);
                IHLOG_DEBUG(ctx, "generating virtual log record deleteLocator# %s", deleteLocator.ToString().data());

                // delete this locator right now; we do not log it, because on recovery it's easy to find
                // duplicate items by their id
                Keeper.Deleter.DeleteDefrag({deleteLocator}, ctx);

                auto it = Keeper.State.Chunks.find(deleteLocator.ChunkIdx);
                Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                TChunkInfo& chunk = it->second;
                Y_ABORT_UNLESS(chunk.DeletedItems.Get(deleteLocator.IndexInsideChunk));
                Y_ABORT_UNLESS(chunk.ChunkSerNum == deleteLocator.ChunkSerNum);
            }

            --InFlightWrites;
            if (ChunkInProgress == deleteLocator.ChunkIdx && ChunkInProgressSerNum == deleteLocator.ChunkSerNum) {
                ProcessIndex(ctx);
            }
        }

        void TDefragmenter::FinishChunkInProgress(const TActorContext& ctx) {
            IHLOG_DEBUG(ctx, "finishing ChunkIdx# %" PRIu32 " ChunkSerNum# %s", ChunkInProgress,
                    ChunkInProgressSerNum.ToString().data());
            ChunkInProgress = 0;
            ChunkInProgressSerNum = {};
            Index.clear();
            ProcessPendingChunksQueue(ctx);
        }

    } // NIncrHuge
} // NKikimr
