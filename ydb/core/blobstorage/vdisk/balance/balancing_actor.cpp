#pragma once

#include "balancing_actor.h"
#include "defs.h"
#include "deleter.h"
#include "merger.h"
#include "sender.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>


namespace NKikimr {

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        TActiveActors ActiveActors;

        TActorId Sender;
        // TDeleter Deleter;

        void Bootstrap(const TActorContext &ctx) {
            CreateVDisksQueues();
            auto [sendOnMainParts, tryDeleteParts] = CollectKeys();

            Sender = ctx.Register(new TSender(std::move(sendOnMainParts), QueueActorMapPtr, Ctx));
            ActiveActors.Insert(Sender, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            Become(&TThis::StateFunc);

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

        std::pair<TQueue<TPartInfo>, TQueue<TPartInfo>> CollectKeys() {
            TQueue<TPartInfo> sendOnMainParts, tryDeleteParts;

            for (It.SeekToFirst(); It.Valid(); It.Next()) {
                TMerger merger(Ctx->GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                auto collectPartsByPredicate = [&](const TVector<ui8>& partIdxs, TQueue<TPartInfo>& queue) {
                    for (ui8 partIdx: partIdxs) {
                        if (!merger.Parts[partIdx].has_value()) {
                            continue;  // something strange
                        }
                        queue.push(TPartInfo{
                            .Key=It.GetCurKey().LogoBlobID(),
                            .PartIdx=partIdx,
                            .PartData=*merger.Parts[partIdx]
                        });
                    }
                };
                collectPartsByPredicate(PartsToSendOnMain(merger.Ingress), sendOnMainParts);
                collectPartsByPredicate(PartsToSendOnMain(merger.Ingress), sendOnMainParts);

                merger.Clear();
            }
            return {sendOnMainParts, tryDeleteParts};
        }

        void StartBalancing() {
            if (!Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId))) {
                HandleReplToken();
            }
        }

        void HandleReplToken() {
            DoJobQuant();
            Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            if (!Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId))) {
                HandleReplToken();
            }
        }

        void DoJobQuant() {
            // Sender.DoJobQuant();
            // Deleter.DoJobQuant();
        }

        TVector<ui8> PartsToSendOnMain(const TIngress& ingress) {
            Y_UNUSED(ingress);
            return {}; // TODO
        }

        TVector<ui8> PartsToDelete(const TIngress& ingress) {
            Y_UNUSED(ingress);
            return {}; // TODO
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvReplToken::EventType, HandleReplToken)
        );

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
