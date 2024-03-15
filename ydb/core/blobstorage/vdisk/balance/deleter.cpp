#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

namespace NKikimr {
namespace NBalancing {

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

        void ScheduleJobQuant(const TActorId& selfId) {
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

                auto vDiskId = GetMainReplicaVDiskId(*GInfo, item.Key);

                // query which would tell us which parts are realy on main (not by ingress)
                auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                    vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, i,
                    {{item.Key.FullID(), 0, 0}}
                );
                ui32 msgSize = ev->CalculateSerializedSize();
                TReplQuoter::QuoteMessage(
                    Quoter,
                    std::make_unique<IEventHandle>(QueueActorMapPtr->at(TVDiskIdShort(vDiskId)), selfId, ev.release()),
                    msgSize
                );
                ++ExpectedResponses;
            }
        }

        std::pair<std::optional<TVector<TPartOnMain>>, ui32> TryGetResults() {
            if (ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return {std::move(Result), Parts.size()};
            }
            return {std::nullopt, Parts.size()};
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            ++Responses;
            auto msg = ev->Get()->Record;
            if (msg.GetStatus() != NKikimrProto::EReplyStatus::OK) {
                return;
            }
            ui64 i = msg.GetCookie();
            auto res = msg.GetResult().at(0);
            for (ui32 partId: res.GetParts()) {
                if (partId == Result[i].Key.PartId()) {
                    Result[i].HasOnMain = true;
                }
            }
        }
    };


    class TDeleter : public TActorBootstrapped<TDeleter> {
        TActorId NotifyId;
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TPartsRequester PartsRequester;
        ui32 OrderId = 0;

        struct TStats {
            ui32 PartsRequested = 0;
            ui32 PartsDecidedToDelete = 0;
            ui32 PartsMarkedDeleted = 0;
        };
        TStats Stats;

        void ScheduleJobQuant() {
            PartsRequester.ScheduleJobQuant(SelfId());
            TryProcessResults();
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            PartsRequester.Handle(ev);
            TryProcessResults();
        }

        void TryProcessResults() {
            if (auto [batch, partsLeft] = PartsRequester.TryGetResults(); batch.has_value()) {
                Stats.PartsRequested += batch->size();
                for (auto& part: *batch) {
                    if (part.HasOnMain) {
                        ++Stats.PartsDecidedToDelete;
                        DeleteLocal(part.Key);
                    }
                }
                Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID, partsLeft));
                if (partsLeft == 0) {
                    PassAway();
                }
            }
        }

        void DeleteLocal(const TLogoBlobID& key) {
            TLogoBlobID keyWithoutPartId(key, 0);

            TIngress ingress;
            ingress.DeleteHandoff(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);

            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Deleting local: " << key.ToString() << " "
                    << ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId));

            Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(keyWithoutPartId, ingress, OrderId++));
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            Y_VERIFY(ev->Get()->OrderId == Stats.PartsMarkedDeleted++);
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID));
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, ScheduleJobQuant)
            hFunc(TEvBlobStorage::TEvVGetResult, Handle)
            hFunc(TEvDelLogoBlobDataSyncLogResult, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)

            hFunc(TEvVGenerationChange, Handle)
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
            , GInfo(ctx->GInfo)
            , PartsRequester(SelfId(), 32, std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, GInfo, queueActorMapPtr)
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

} // NBalancing
} // NKikimr
