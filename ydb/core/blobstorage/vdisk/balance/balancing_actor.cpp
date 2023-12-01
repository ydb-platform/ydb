#pragma once

#include "balancing_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

    class TKeysHandler {
        virtual void Process(ui32 batchSize) = 0;
    };

    class TSender {
    private:
        TQueue<TLogoBlobID> Keys;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
    
    public:
        TSender(
            TQueue<TLogoBlobID> keys,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : Keys(std::move(keys))
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
        {}
    };

    class TDeleter {
    private:
        TQueue<TLogoBlobID> Keys;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
    
    public:
        TDeleter(
            TQueue<TLogoBlobID> keys,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : Keys(std::move(keys))
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
        {}
    };

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TLogoBlobsSnapshot::TIndexForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        TActiveActors ActiveActors;

        TSender Sender;
        TDeleter Deleter;

        void Bootstrap() {
            CreateVDisksQueues();
            auto [sendOnMainKeys, tryDeleteKeys] = CollectKeys();
            Sender = TSender(sendOnMainKeys, QueueActorMapPtr, Ctx);
            Deleter = TDeleter(sendOnMainKeys, QueueActorMapPtr, Ctx);


            // Ctx->VCtx->ReplNodeRequestQuoter
        }

        void CreateVDisksQueues() {
            QueueActorMapPtr = std::make_shared<TQueueActorMap>();
            auto interconnectChannel = static_cast<TInterconnectChannels::EInterconnectChannels>(
                    Ctx->VDiskCfg->ReplInterconnectChannel);
            const TBlobStorageGroupInfo::TTopology& topology = Ctx->GInfo->GetTopology();
            NBackpressure::TQueueClientId replQueueClientId(
                NBackpressure::EQueueClientType::ReplJob, topology.GetOrderNumber(Ctx->VCtx->ShortSelfVDisk));

            CreateQueuesForVDisks(*QueueActorMapPtr, SelfId(), Ctx->GInfo, Ctx->VCtx,
                    Ctx->GInfo->GetVDisks(), Ctx->MonGroup.GetGroup(),
                    replQueueClientId, NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
                    "DisksBalancing", interconnectChannel);
        }

        std::pair<TQueue<TLogoBlobID>, TQueue<TLogoBlobID>> CollectKeys() {
            // TBlobIdQueuePtr
            TQueue<TLogoBlobID> sendOnMainKeys, tryDeleteKeys;
            for (It.SeekToFirst(); It.Valid(); It.Next()) {
                const TLogoBlobID& key = It.GetCurKey().LogoBlobID();
                const TMemRecLogoBlob& rec = It.GetMemRec();

                // TDiskDataExtractor extr;
                // extr = rec.GetDiskData(&extr, const TDiskPart *outbound)
                
                if (!MainHasPart(rec.GetIngress())) {
                    sendOnMainKeys.push(key);
                } else if (AllReplicasHasParts(rec.GetIngress())) {
                    tryDeleteKeys.push(key);
                }
            }
            return {sendOnMainKeys, tryDeleteKeys};
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
