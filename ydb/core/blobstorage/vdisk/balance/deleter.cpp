#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NBalancing {

namespace {
    struct TPartOnMain {
        TLogoBlobID Key;
        bool HasOnMain;
    };

    class TPartsRequester {
    private:
        const TActorId NotifyId;
        const size_t BatchSize;
        TQueue<TLogoBlobID> Parts;
        TReplQuoter::TPtr Quoter;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQueueActorMapPtr QueueActorMapPtr;
        NMonGroup::TBalancingGroup& MonGroup;

        TVector<TPartOnMain> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:

        TPartsRequester(TActorId notifyId, size_t batchSize, TQueue<TLogoBlobID> parts, TReplQuoter::TPtr quoter, TIntrusivePtr<TBlobStorageGroupInfo> gInfo, TQueueActorMapPtr queueActorMapPtr, NMonGroup::TBalancingGroup& monGroup)
            : NotifyId(notifyId)
            , BatchSize(batchSize)
            , Parts(std::move(parts))
            , Quoter(quoter)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
            , MonGroup(monGroup)
            , Result(Reserve(BatchSize))
            , Responses(0)
            , ExpectedResponses(0)
        {}

        void ScheduleJobQuant(const TActorId& selfId) {
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            THashMap<TVDiskID, std::unique_ptr<TEvBlobStorage::TEvVGet>> vDiskToQueries;

            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto key = Parts.front();
                Parts.pop();
                Result[i] = TPartOnMain{
                    .Key=key,
                    .HasOnMain=false
                };

                auto vDiskId = GetMainReplicaVDiskId(*GInfo, key);

                auto& ev = vDiskToQueries[vDiskId];
                if (!ev) {
                    ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                        vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                        TEvBlobStorage::TEvVGet::EFlags::None, 0
                    );
                }

                ev->AddExtremeQuery(key.FullID(), 0, 0, &i);
            }

            for (auto& [vDiskId, ev]: vDiskToQueries) {
                ui32 msgSize = ev->CalculateSerializedSize();
                TReplQuoter::QuoteMessage(
                    Quoter,
                    std::make_unique<IEventHandle>(QueueActorMapPtr->at(TVDiskIdShort(vDiskId)), selfId, ev.release()),
                    msgSize
                );
                ++MonGroup.CandidatesToDeleteAskedFromMain();
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
            ++MonGroup.CandidatesToDeleteAskedFromMainResponse();
            auto msg = ev->Get()->Record;
            if (msg.GetStatus() != NKikimrProto::EReplyStatus::OK) {
                return;
            }

            for (const auto& res: msg.GetResult()) {
                auto i = res.GetCookie();
                Y_DEBUG_ABORT_UNLESS(i < Result.size());
                for (ui32 partId: res.GetParts()) {
                    if (partId == Result[i].Key.PartId()) {
                        Result[i].HasOnMain = true;
                    }
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
        TWaiter Mngr;

        struct TStats {
            ui32 PartsRequested = 0;
            ui32 PartsDecidedToDelete = 0;
            ui32 PartsMarkedDeleted = 0;
        };
        TStats Stats;

        void ScheduleJobQuant() {
            Mngr.Init();
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
                ui64 epoch = Mngr.GetEpoch();
                ui32 deletedParts = 0;
                for (auto& part: *batch) {
                    if (part.HasOnMain) {
                        ++Stats.PartsDecidedToDelete;
                        DeleteLocal(part.Key, epoch);
                        ++deletedParts;
                    }
                }
                Mngr.StartJob(TlsActivationContext->Now(), deletedParts, partsLeft);
                Schedule(Mngr.SendTimeout, new NActors::TEvents::TEvCompleted(epoch));
                TryCompleteBatch(epoch);
            }
        }

        void TryCompleteBatch(NActors::TEvents::TEvCompleted::TPtr ev) {
            TryCompleteBatch(ev->Get()->Id);
        }

        void TryCompleteBatch(ui64 epoch) {
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB10, VDISKP(Ctx->VCtx, "TEvDelLogoBlobDataSyncLogResult received"), (ExpectedEpoch, epoch), (ActualEpoch, Mngr.GetEpoch()), (ToSendPartsCount, Mngr.ToSendPartsCount), (SentPartsCount, Mngr.SentPartsCount));

            if (auto ev = Mngr.IsJobDone(epoch, TlsActivationContext->Now()); ev != nullptr) {
                Send(NotifyId, ev);
                if (Mngr.IsPassAway()) {
                    PassAway();
                }
            }
        }

        void DeleteLocal(const TLogoBlobID& key, ui64 epoch) {
            TLogoBlobID keyWithoutPartId(key, 0);

            TIngress ingress;
            ingress.DeleteHandoff(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB10, VDISKP(Ctx->VCtx, "Deleting local"), (LogoBlobID, key.ToString()),
                (Ingress, ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId)));

            Send(Ctx->SkeletonId, new TEvDelLogoBlobDataSyncLog(keyWithoutPartId, ingress, OrderId++, epoch));

            ++Ctx->MonGroup.MarkedReadyToDelete();
            Ctx->MonGroup.MarkedReadyToDeleteBytes() += GInfo->GetTopology().GType.PartSize(key);
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            Stats.PartsMarkedDeleted++;
            ++Ctx->MonGroup.MarkedReadyToDeleteResponse();
            Ctx->MonGroup.MarkedReadyToDeleteWithResponseBytes() += GInfo->GetTopology().GType.PartSize(ev->Get()->Id);
            ui64 epoch = ev->Get()->Cookie;
            Mngr.PartJobDone(epoch);
            TryCompleteBatch(epoch);
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
            hFunc(NActors::TEvents::TEvCompleted, TryCompleteBatch)

            hFunc(TEvVGenerationChange, Handle)
        );

    public:
        TDeleter() = default;
        TDeleter(
            TActorId notifyId,
            TQueue<TLogoBlobID> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , PartsRequester(SelfId(), 32, std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, GInfo, queueActorMapPtr, Ctx->MonGroup)
            , Mngr{.ServiceId=DELETER_ID}
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}

IActor* CreateDeleterActor(
    TActorId notifyId,
    TQueue<TLogoBlobID> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TDeleter(notifyId, std::move(parts), queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
