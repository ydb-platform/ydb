#pragma once

#include "balancing_actor.h"

#include <ydb/core/blobstorage/hulldb/hull_ds_all_snap.h>


namespace NKikimr {

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TLogoBlobsSnapshot::TIndexForwardIterator It;

        void Bootstrap() {
            TQueue<TLogoBlobID> sendOnMainKeys, tryDeleteKeys;

            for (It.SeekToFirst(); It.Valid(); It.Next()) {
                const TLogoBlobID& key = It.GetCurKey().LogoBlobID();
                const TMemRecLogoBlob& rec = It.GetMemRec();
                
                if (!MainHasPart(rec.GetIngress())) {
                    sendOnMainKeys.push(key);
                } else if (AllReplicasHasParts(rec.GetIngress())) {
                    tryDeleteKeys.push(key);
                }
            }

            // Ctx->VCtx->ReplNodeRequestQuoter
        }

        bool MainHasPart(const TIngress& ingress) {
            Y_UNUSED(ingress);
            return true; // TODO
        }

        bool AllReplicasHasParts(const TIngress& ingress) {
            Y_UNUSED(ingress);
            return true; // TODO
        }

    public:
        TBalancingActor(std::shared_ptr<TBalancingCtx> &ctx)
            : TActorBootstrapped<TBalancingActor>()
            , Ctx(ctx)
            , It(Ctx->Snap.HullCtx, &Ctx->Snap.LogoBlobsSnap)
        {
        }
    };

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> &ctx) {
        return new TBalancingActor(ctx);
    }
} // NKikimr
