#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

namespace NKikimr {

namespace {
    class TPartsRequester {
    private:
        const TActorId NotifyId;
        const size_t BatchSize;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQueueActorMapPtr QueueActorMapPtr;

        TVector<TPartOnMain> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:
        enum EState {
            WAITING_RESPONSES,
            FINISHED,
        };

        TPartsRequester(TActorId notifyId, size_t batchSize, TQueue<TPartInfo> parts, TReplQuoter::TPtr quoter, TIntrusivePtr<TBlobStorageGroupInfo> gInfo, TQueueActorMapPtr queueActorMapPtr)
            : NotifyId(notifyId)
            , BatchSize(batchSize)
            , Parts(std::move(parts))
            , Quoter(quoter)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
            , Result(Reserve(BatchSize))
            , Responses(0)
            , ExpectedResponses(0)
        {}

        EState DoJobQuant(const TActorContext &ctx) {
            if (ExpectedResponses != 0) {
                return WAITING_RESPONSES;
            }
            if (Parts.empty()) {
                return FINISHED;
            }
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto item = Parts.front();
                Parts.pop();
                Result[i] = TPartOnMain{
                    .Key=item.Key,
                    .Ingress=item.Ingress,
                    .HasOnMain=false
                };

                auto vDiskId = GetVDiskId(*GInfo, item.Key);
                auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                    vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, i,
                    {{item.Key.FullID(), 0, 0}}
                );
                TReplQuoter::QuoteMessage(
                    Quoter,
                    std::make_unique<IEventHandle>(QueueActorMapPtr->at(TVDiskIdShort(vDiskId)), ctx.SelfID, ev.release()),
                    0
                );
                ++ExpectedResponses;
            }
            return WAITING_RESPONSES;
        }

        std::optional<TVector<TPartOnMain>> TryGetResults() {
            if (ExpectedResponses != 0 && ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return Result;
            }
            return std::nullopt;
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            ++Responses;
            auto msg = ev->Get()->Record;
            if (msg.GetStatus() != NKikimrProto::EReplyStatus::OK) {
                return;
            }
            ui64 i = msg.GetCookie();
            auto res = msg.GetResult().at(0);
            for (ui32 partIdx: res.GetParts()) {
                if (partIdx == Result[i].Key.PartId()) {
                    Result[i].HasOnMain = true;
                }
            }
        }
    };


    class TDeleter : public TActorBootstrapped<TDeleter> {
        TActorId NotifyId;
        std::shared_ptr<TBalancingCtx> Ctx;
        TPartsRequester PartsRequester;
        ui32 OrderId = 0;

        struct TStats {
            ui32 PartsRequested = 0;
            ui32 PartsDecidedToDelete = 0;
            ui32 PartsMarkedDeleted = 0;
        };
        TStats Stats;

        void DoJobQuant(const TActorContext &ctx) {
            auto status = PartsRequester.DoJobQuant(ctx);
            if (auto batch = PartsRequester.TryGetResults()) {
                Stats.PartsRequested += batch->size();
                for (auto& part: *batch) {
                    if (part.HasOnMain) {
                        ++Stats.PartsDecidedToDelete;
                        DeleteLocal(ctx, part.Key, part.Ingress);
                    }
                }
            }
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "PartsRequester status " << ": " << (ui32)status << " " << Stats.PartsDecidedToDelete);
            if (status == TPartsRequester::EState::FINISHED && Stats.PartsDecidedToDelete == Stats.PartsMarkedDeleted) {
                Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID));
                Send(SelfId(), new NActors::TEvents::TEvPoison);
            }
        }

        void DeleteLocal(const TActorContext &ctx, const TLogoBlobID& key, const TIngress& ingress) {
            TLogoBlobID keyWithoutPartId(key, 0);

            TIngress patchedIngress = ingress.CopyWithoutLocal(Ctx->GInfo->GetTopology().GType);
            patchedIngress.DeleteHandoff(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);

            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " 
                 << "Deleting local: " << keyWithoutPartId.ToString() << " " << patchedIngress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId));

            ctx.Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(keyWithoutPartId, patchedIngress, OrderId++));
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            Y_VERIFY(ev->Get()->OrderId == Stats.PartsMarkedDeleted++);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(TEvBlobStorage::TEvVGetResult, PartsRequester.Handle)
            hFunc(TEvDelLogoBlobDataSyncLogResult, Handle)
            CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        );

    public:
        TDeleter() = default;
        TDeleter(
            TActorId notifyId,
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , Ctx(ctx)
            , PartsRequester(SelfId(), 32, std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, Ctx->GInfo, queueActorMapPtr)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}

IActor* CreateDeleterActor(
    TActorId notifyId,
    TQueue<TPartInfo> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TDeleter(notifyId, parts, queueActorMapPtr, ctx);
}

}
