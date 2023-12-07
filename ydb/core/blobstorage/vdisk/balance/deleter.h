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
        std::shared_ptr<TBalancingCtx> Ctx;
        TPartsRequester PartsRequester;
        ui32 OrderId = 0;
        ui32 DoneOrderId = 0;

        void DoJobQuant(const TActorContext &ctx) {
            if (auto batch = PartsRequester.TryGetResults()) {
                for (auto& part: *batch) {
                    if (part.HasOnMain) {
                        DeleteLocal(ctx, part.Key, part.Ingress);
                    }
                }
            }
            PartsRequester.DoJobQuant(ctx);
        }

        void DeleteLocal(const TActorContext &ctx, const TLogoBlobID& key, TIngress& ingress) {
            ingress.DeleteHandoff(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);
            ingress.LocalParts(Ctx->GInfo->GetTopology().GType).Clear(key.PartId() - 1);
            ctx.Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(key, ingress, OrderId++));
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            Y_VERIFY(ev->Get()->OrderId == DoneOrderId++);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(TEvBlobStorage::TEvVGetResult, PartsRequester.Handle)
            hFunc(TEvDelLogoBlobDataSyncLogResult, Handle)
        );

    public:
        TDeleter() = default;
        TDeleter(
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : Ctx(ctx)
            , PartsRequester(SelfId(), 32, std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, queueActorMapPtr)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}
