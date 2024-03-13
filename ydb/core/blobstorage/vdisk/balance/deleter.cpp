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

        EState DoJobQuant(const TActorId& selfId) {
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

                auto vDiskId = GetMainReplicaVDiskId(*GInfo, item.Key);
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
        TPartsRequester PartsRequester;
        ui32 OrderId = 0;

        struct TStats {
            ui32 PartsRequested = 0;
            ui32 PartsDecidedToDelete = 0;
            ui32 PartsMarkedDeleted = 0;
        };
        TStats Stats;

        void DoJobQuant() {
            auto status = PartsRequester.DoJobQuant(SelfId());
            if (auto batch = PartsRequester.TryGetResults()) {
                Stats.PartsRequested += batch->size();
                for (auto& part: *batch) {
                    if (part.HasOnMain) {
                        ++Stats.PartsDecidedToDelete;
                        DeleteLocal(part.Key);
                    }
                }
            }
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "PartsRequester status " << ": " << (ui32)status << " " << Stats.PartsDecidedToDelete);
            if (status == TPartsRequester::EState::FINISHED && Stats.PartsDecidedToDelete == Stats.PartsMarkedDeleted) {
                PassAway();
            }
        }

        void DeleteLocal(const TLogoBlobID& key) {
            TLogoBlobID keyWithoutPartId(key, 0);

            TIngress ingress;
            ingress.DeleteHandoff(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);

            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Deleting local: " << key.ToString() << " "
                    << ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId));

            Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(keyWithoutPartId, ingress, OrderId++));
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            Y_VERIFY(ev->Get()->OrderId == Stats.PartsMarkedDeleted++);
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID));
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(TEvBlobStorage::TEvVGetResult, PartsRequester.Handle)
            hFunc(TEvDelLogoBlobDataSyncLogResult, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
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

} // NBalancing
} // NKikimr
