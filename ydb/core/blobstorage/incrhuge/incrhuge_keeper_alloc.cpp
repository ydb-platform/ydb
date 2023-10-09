#include "incrhuge_keeper_alloc.h"
#include "incrhuge_keeper.h"
#include "incrhuge_keeper_write.h"

namespace NKikimr {
    namespace NIncrHuge {

        TAllocator::TAllocator(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Allocator")
        {}

        TAllocator::~TAllocator()
        {}

        void TAllocator::CheckForAllocationNeed(const TActorContext& ctx) {
            // if there is a query, we exit; we allow only one uncompleted query at a time
            if (ChunkReserveInFlight) {
                return;
            }

            // calculate number of chunks we currently have (the one being written to + write intent queue + uncommitted)
            ui32 numChunks = Keeper.State.WriteIntentQueue.size() + NumReservedUncommittedChunks;
            if (Keeper.State.CurrentChunk) {
                ++numChunks;
            }

            // see if we potentially need to allocate something
            if (numChunks < Keeper.State.Settings.MinCleanChunks) {
                // calculate number of chunks we have to reserve
                ui32 numToReserve = Keeper.State.Settings.MinCleanChunks - numChunks;
                // reserve only if we exceed specified allocation batch
                if (numToReserve >= Keeper.State.Settings.MinAllocationBatch) {
                    ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvChunkReserve(Keeper.State.PDiskParams->Owner,
                            Keeper.State.PDiskParams->OwnerRound, numToReserve));
                    ChunkReserveInFlight = true;
                }
            }
        }

        void TAllocator::ApplyAllocate(NKikimrProto::EReplyStatus status, TVector<TChunkIdx>&& chunks,
                const TActorContext& ctx) {
            if (status == NKikimrProto::OK) {
                // count these chunks as reserved, but not yet committed
                NumReservedUncommittedChunks += chunks.size();

                // remember current id to store it in intent queue in case of success
                TChunkSerNum baseSerNum = Keeper.State.CurrentSerNum;
                Keeper.State.CurrentSerNum.Advance(chunks.size());

                // create callback which is invoked when allocation logging is finished; it captures a copy of
                // "chunks" vector to put it into write intent queue in case of success
                auto callback = [this, chunks, baseSerNum](NKikimrProto::EReplyStatus status,
                        IEventBase* /*result*/, const TActorContext& ctx) {
                    ChunkReserveInFlight = false;
                    Y_ABORT_UNLESS(NumReservedUncommittedChunks >= chunks.size());
                    NumReservedUncommittedChunks -= chunks.size();
                    if (status == NKikimrProto::OK) {
                        // copy just allocated and confirmed chunks to write intent queue and kick writer (it may be
                        // waiting for new chunks to arrive); we assign sequential identifiers to each allocated chunk
                        // to enable strict ordering of write intent queue on recovery
                        for (size_t i = 0; i < chunks.size(); ++i) {
                            TChunkSerNum chunkSerNum = baseSerNum.Add(i);
                            const ui32 chunkIdx = chunks[i];
                            IHLOG_DEBUG(ctx, "ChunkIdx# %" PRIu32 " ChunkSerNum# %s", chunkIdx, chunkSerNum.ToString().data());
                            Keeper.State.WriteIntentQueue.push(chunks[i]);
                            Keeper.State.Chunks.emplace(chunks[i], TChunkInfo{
                                    EChunkState::WriteIntent, // State
                                    chunkSerNum,              // ChunkSerNum
                                    0,                        // NumUsedBlocks
                                    0,                        // NumItems
                                    {},                       // DeletedItems
                                    0,                        // InFlightReq
                                });
                        }
                        Keeper.Writer.ProcessWriteQueue(ctx);
                    }
                    CheckForAllocationNeed(ctx);
                };

                // log allocation which will record new chunk and new current id
                Keeper.Logger.LogChunkAllocation(std::move(chunks), baseSerNum, MakeCallback(std::move(callback)), ctx);
            } else {
                // try again, what else we can do?
                ChunkReserveInFlight = false;
                CheckForAllocationNeed(ctx);
            }
        }

    } // NIncrHuge
} // NKikimr
