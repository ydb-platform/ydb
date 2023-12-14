#pragma once

#include "defs.h"
#include "parts_requester.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>


namespace NKikimr {

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
            Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "PartsRequester status " << ": " << (ui32)status << Endl;
            if (status == TPartsRequester::EState::FINISHED && Stats.PartsDecidedToDelete == Stats.PartsMarkedDeleted) {
                Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID));
                Send(SelfId(), new NActors::TEvents::TEvPoison);
            }
        }

        void DeleteLocal(const TActorContext &ctx, const TLogoBlobID& key, TIngress& ingress) {
            ingress.DeleteHandoff(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);
            ingress.LocalParts(Ctx->GInfo->GetTopology().GType).Clear(key.PartId() - 1);
            ctx.Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(key, ingress, OrderId++));
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
            , PartsRequester(SelfId(), 32, std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, queueActorMapPtr)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}
