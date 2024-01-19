#include "query_readbatch.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <util/generic/algorithm.h>

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
            if (IsRepl) {
                quoter = Ctx->VCtx->ReplPDiskReadQuoter;
            }

            // make read requests
            for (TGlueReads::iterator it = Result->GlueReads.begin(), e = Result->GlueReads.end(); it != e; ++it) {
                // cookie for this request
                void *cookie = &*it;

                // create request
                std::unique_ptr<NPDisk::TEvChunkRead> msg(new NPDisk::TEvChunkRead(Ctx->PDiskCtx->Dsk->Owner,
                            Ctx->PDiskCtx->Dsk->OwnerRound, it->Part.ChunkIdx, it->Part.Offset, it->Part.Size,
                            Priority, cookie));

                LOG_DEBUG(ctx, BS_VDISK_GET,
                    VDISKP(Ctx->VCtx->VDiskLogPrefix, "GLUEREAD(%p): %s", this, msg->ToString().data()));

                // send request
                TReplQuoter::QuoteMessage(quoter, std::make_unique<IEventHandle>(Ctx->PDiskCtx->PDiskId, SelfId(),
                    msg.release(), 0, 0, nullptr, Span.GetTraceId()), it->Part.Size);

                Counter++;
            }
        }

        void Finish(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_VDISK_GET,
                VDISKP(Ctx->VCtx->VDiskLogPrefix, "GLUEREAD FINISHED(%p): actualReadN# %" PRIu32
                    " origReadN# %" PRIu32, this, ui32(Result->GlueReads.size()),
                    ui32(Result->DiskDataItemPtrs.size())));
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
            glueRead->Data = std::move(msg->Data);
            glueRead->Success = status == NKikimrProto::OK;

            Counter--;
            if (Counter == 0)
                Finish(ctx);
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
