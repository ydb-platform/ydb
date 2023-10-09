#include "incrhuge_keeper_read.h"
#include "incrhuge_keeper.h"

namespace NKikimr {
    namespace NIncrHuge {

        TReader::TReader(TKeeper& keeper)
            : TKeeperComponentBase(keeper, "Reader")
        {}

        TReader::~TReader()
        {}

        void TReader::HandleRead(TEvIncrHugeRead::TPtr& ev, const TActorContext& ctx) {
            TEvIncrHugeRead *msg = ev->Get();

            // fill queue item
            TReadQueueItem item{msg->Owner, msg->Id, msg->Offset, msg->Size, ev->Sender, ev->Cookie};

            // try to process it unless queue already has items
            if (ReadQueue || !ProcessReadItem(item, ctx)) {
                ReadQueue.push(std::move(item));
            }
        }

        bool TReader::ProcessReadItem(TReadQueueItem& item, const TActorContext& ctx) {
            const TBlobLocator& locator = Keeper.State.BlobLookup.Lookup(item.Id);

            // verify owner
            Y_ABORT_UNLESS(locator.Owner == item.Owner);

            // calculate size we want to read
            const ui32 maxSize = item.Offset < locator.PayloadSize ? locator.PayloadSize - item.Offset : 0;
            const ui32 size = Min(maxSize, item.Size ? item.Size : maxSize);

            // if there is no data, reply now
            if (!size) {
                ctx.Send(item.Sender, new TEvIncrHugeReadResult(NKikimrProto::OK, {}), 0, item.Cookie);
                return true;
            }

            // calculate offset inside chunk (in bytes)
            ui32 offsetInChunk = locator.OffsetInBlocks * Keeper.State.BlockSize + sizeof(TBlobHeader) + item.Offset;

            // callback
            auto callback = [this, chunkIdx = locator.ChunkIdx, sender = item.Sender, cookie = item.Cookie]
                    (NKikimrProto::EReplyStatus status, IEventBase *msg, const TActorContext& ctx) {
                auto it = Keeper.State.Chunks.find(chunkIdx);
                Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
                TChunkInfo& chunk = it->second;
                --chunk.InFlightReq;
                if (!chunk.InFlightReq && chunk.State == EChunkState::Deleting) {
                    Keeper.Deleter.IssueLogChunkDelete(chunkIdx, ctx);
                }

                TString data;
                NPDisk::TEvChunkReadResult& result = *static_cast<NPDisk::TEvChunkReadResult*>(msg);
                if (result.Data.IsReadable()) {
                    data = TStringBuf(result.Data.ToString());
                } else {
                    status = NKikimrProto::ERROR;
                }
                ctx.Send(sender, new TEvIncrHugeReadResult(status, std::move(data)), 0, cookie);
            };

            // send request to yard
            ctx.Send(Keeper.State.Settings.PDiskActorId, new NPDisk::TEvChunkRead(Keeper.State.PDiskParams->Owner,
                    Keeper.State.PDiskParams->OwnerRound, locator.ChunkIdx, offsetInChunk, size,
                    NPriRead::HullOnlineOther, Keeper.RegisterYardCallback(MakeCallback(std::move(callback)))));

            // find chunk and spin request counter
            auto it = Keeper.State.Chunks.find(locator.ChunkIdx);
            Y_ABORT_UNLESS(it != Keeper.State.Chunks.end());
            TChunkInfo& chunk = it->second;
            Y_ABORT_UNLESS(chunk.State != EChunkState::Deleting);
            ++chunk.InFlightReq;

            return true;
        }

        void TReader::ProcessReadQueue(const TActorContext& ctx) {
            while (ReadQueue && ProcessReadItem(ReadQueue.front(), ctx)) {
                ReadQueue.pop();
            }
        }

    } // NIncrHuge
} // NKikimr
