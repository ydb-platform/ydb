#include "query_readbatch.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_format_context.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_GET

using namespace NKikimrServices;

namespace NKikimr {

    using namespace NReadBatcher;

    ////////////////////////////////////////////////////////////////////////////
    // TTReadBatcherActor
    ////////////////////////////////////////////////////////////////////////////
    class TTReadBatcherActor : public TActorBootstrapped<TTReadBatcherActor> {
        TReadBatcherCtxPtr Ctx;
        const TActorId NotifyID;
        std::shared_ptr<TReadBatcherResult> Result;
        const ui8 Priority;
        NWilson::TSpan Span;
        const bool IsRepl;
        ui32 Counter = 0;

        friend class TActorBootstrapped<TTReadBatcherActor>;

        void Bootstrap(const TActorContext &ctx) {
            Become(&TThis::StateFunc);
            Y_DEBUG_ABORT_UNLESS(!Result->GlueReads.empty());

            TReplQuoter::TPtr quoter;
            NMonitoring::TDynamicCounters::TCounterPtr quoterThrottledCounter;
            if (IsRepl) {
                quoter = Ctx->VCtx->ReplPDiskReadQuoter;
                quoterThrottledCounter = Ctx->ReplMonGroup.ReplPDiskReadThrottledMicrosecondsPtr();
            }

            // make read requests
            for (TGlueReads::iterator it = Result->GlueReads.begin(), e = Result->GlueReads.end(); it != e; ++it) {
                // cookie for this request
                void *cookie = &*it;

                // create request
                std::unique_ptr<NPDisk::TEvChunkRead> msg(new NPDisk::TEvChunkRead(Ctx->PDiskCtx->Dsk->Owner,
                    Ctx->PDiskCtx->Dsk->OwnerRound, it->Part.ChunkIdx, it->Part.Offset, it->Part.Size,
                    Priority, cookie));

                msg->BlobId = it->HugeBlobId;
                // Whole raw sectors, to be checked and opened here rather than in the PDisk completion thread.
                msg->RawSectors = UseRawSectors();

                YDB_LOG_DEBUG_CTX(ctx, VDISKP(Ctx->VCtx->VDiskLogPrefix, "GLUEREAD(%p): %s", this, msg->ToString().data()));

                // send request
                TReplQuoter::QuoteMessage(quoter, std::make_unique<IEventHandle>(Ctx->PDiskCtx->PDiskId, SelfId(),
                    msg.release(), 0, 0, nullptr, Span.GetTraceId()), it->Part.Size, 0, quoterThrottledCounter);

                Counter++;
            }
        }

        void Finish(const TActorContext &ctx) {
            YDB_LOG_DEBUG_CTX(ctx, VDISKP(Ctx->VCtx->VDiskLogPrefix, "GLUEREAD FINISHED(%p): actualReadN# %" PRIu32 " origReadN# %" PRIu32, this, ui32(Result->GlueReads.size()), ui32(Result->DiskDataItemPtrs.size())));
            ctx.Send(NotifyID, new TEvents::TEvCompleted);
            Span.EndOk();
            Die(ctx);
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr &ev, const TActorContext &ctx) {
            TString message;
            const NKikimrProto::EReplyStatus status = ev->Get()->Status;
            if (status != NKikimrProto::OK) {
                TStringStream str;
                str << "{TEvChunkReadResult# " << ev->Get()->ToString() << " GlueReads# [";
                for (auto it = Result->GlueReads.begin(); it != Result->GlueReads.end(); ++it) {
                    str << (it != Result->GlueReads.begin() ? " " : "") << it->Part.ToString();
                }
                str << "] OrigEv# " << Ctx->OrigEv->Get()->ToString() << " DataItems# [";
                for (auto it = Result->DataItems.begin(); it != Result->DataItems.end(); ++it) {
                    str << (it != Result->DataItems.begin() ? " " : "") << it->ToString();
                }
                str << "]}";
                message = str.Str();
            }
            if (status != NKikimrProto::CORRUPTED) {
                CHECK_PDISK_RESPONSE_MSG(Ctx->VCtx, ev, ctx, message);
            }

            NPDisk::TEvChunkReadResult *msg = ev->Get();

            TGlueRead *glueRead = static_cast<TGlueRead *>(msg->Cookie);
            if (status == NKikimrProto::OK && msg->RawSectors) {
                glueRead->Data = OpenRawSectors(*glueRead, *msg);
            } else {
                glueRead->Data = std::move(msg->Data);
            }
            glueRead->Success = status == NKikimrProto::OK;

            Counter--;
            if (Counter == 0)
                Finish(ctx);
        }

        bool UseRawSectors() const {
            const auto& pdiskCtx = *Ctx->PDiskCtx;
            return pdiskCtx.FormatContext && !pdiskCtx.FormatContext->IsPlainDataChunks()
                && static_cast<i64>(pdiskCtx.EnableSectorPreparation) != 0;
        }

        // Checks and decrypts the raw sectors of a RawSectors read into the payload the request asked for. A sector
        // that does not check out becomes a gap, as when the PDisk itself checks the sectors.
        TBufferWithGaps OpenRawSectors(const TGlueRead& glueRead, NPDisk::TEvChunkReadResult& msg) {
            const NPDisk::TFormatContextPtr& format = Ctx->PDiskCtx->FormatContext;
            const ui32 payloadSize = format->GetSectorPayloadSize();
            const ui32 sectorSize = format->GetSectorSize();
            const ui32 offset = glueRead.Part.Offset;
            const ui32 size = glueRead.Part.Size;
            const ui32 firstSector = offset / payloadSize;
            Y_ABORT_UNLESS(msg.Offset == firstSector * sectorSize);

            // The buffer was read for this request alone, so it is decrypted in place.
            const ui32 sectors = (offset + size - 1) / payloadSize - firstSector + 1;
            ui8 *raw = msg.Data.RawDataPtr(0, sectors * sectorSize);
            TRcBuf data = TRcBuf::UninitializedPageAligned(size);
            NPDisk::TFormatContext::TOpener opener(format, glueRead.Part.ChunkIdx, msg.ChunkNonce, glueRead.HugeBlobId);
            TBufferWithGaps result(offset);
            ui32 done = 0;
            for (ui32 sector = firstSector; done < size; ++sector) {
                const ui32 from = sector == firstSector ? offset % payloadSize : 0;
                const ui32 len = Min(payloadSize - from, size - done);
                ui8 *dst = reinterpret_cast<ui8*>(data.GetDataMut()) + done;
                if (!opener.Open(dst, raw + (ui64)(sector - firstSector) * sectorSize, sector, from, len)) {
                    memset(dst, 0, len);
                    result.AddGap(sector * payloadSize, (sector + 1) * payloadSize);
                }
                done += len;
            }
            result.SetData(std::move(data));
            return result;
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Span.EndError("EvPoisonPill");
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvChunkReadResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_READ_BATCHER;
        }

        TTReadBatcherActor(
                TReadBatcherCtxPtr ctx,
                const TActorId notifyID,
                std::shared_ptr<TReadBatcherResult> result,
                ui8 priority,
                NWilson::TTraceId traceId,
                bool isRepl)
            : TActorBootstrapped<TTReadBatcherActor>()
            , Ctx(ctx)
            , NotifyID(notifyID)
            , Result(std::move(result))
            , Priority(priority)
            , Span(TWilson::VDiskInternals, std::move(traceId), "VDisk.Query.ReadBatcher")
            , IsRepl(isRepl)
        {}
    };

    IActor *CreateReadBatcherActor(
        TReadBatcherCtxPtr ctx,
        const TActorId notifyID,
        std::shared_ptr<TReadBatcherResult> result,
        ui8 priority,
        NWilson::TTraceId traceId,
        bool isRepl)
    {
        return new TTReadBatcherActor(ctx, notifyID, result, priority, std::move(traceId), isRepl);
    }

} // NKikimr
