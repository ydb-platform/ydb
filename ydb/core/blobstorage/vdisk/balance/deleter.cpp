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
        TConstArrayRef<TLogoBlobID> Parts;
        TReplQuoter::TPtr Quoter;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQueueActorMapPtr QueueActorMapPtr;
        NMonGroup::TBalancingGroup& MonGroup;

        TVector<TPartOnMain> Result;
        ui32 RequestsSent = 0;
        ui32 Responses = 0;
    public:

        TPartsRequester(TActorId notifyId, TConstArrayRef<TLogoBlobID> parts, TReplQuoter::TPtr quoter, TIntrusivePtr<TBlobStorageGroupInfo> gInfo, TQueueActorMapPtr queueActorMapPtr, NMonGroup::TBalancingGroup& monGroup)
            : NotifyId(notifyId)
            , Parts(parts)
            , Quoter(quoter)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
            , MonGroup(monGroup)
            , Result(parts.size())
        {
        }

        void SendRequestsToCheckPartsOnMain(const TActorId& selfId) {
            THashMap<TVDiskID, std::unique_ptr<TEvBlobStorage::TEvVGet>> vDiskToQueries;

            for (ui64 i = 0; i < Parts.size(); ++i) {
                auto key = Parts[i];
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
                ++MonGroup.CandidatesToDeleteAskedFromMain();
            }

            for (auto& [vDiskId, ev]: vDiskToQueries) {
                ui32 msgSize = ev->CalculateSerializedSize();
                TReplQuoter::QuoteMessage(
                    Quoter,
                    std::make_unique<IEventHandle>(QueueActorMapPtr->at(TVDiskIdShort(vDiskId)), selfId, ev.release()),
                    msgSize
                );
                ++RequestsSent;
            }
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            ++Responses;
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
                ++MonGroup.CandidatesToDeleteAskedFromMainResponse();
            }
        }

        ui32 GetPartsSize() const {
            return Parts.size();
        }

        bool IsDone() const {
            return Responses == RequestsSent;
        }

        const TVector<TPartOnMain>& GetResult() const {
            return Result;
        }
    };

    class TPartsDeleter {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        ui32 OrderId = 0;

        ui32 RequestsSent = 0;
        ui32 Responses = 0;
    public:
        TPartsDeleter(std::shared_ptr<TBalancingCtx> ctx, TIntrusivePtr<TBlobStorageGroupInfo> gInfo)
            : Ctx(ctx)
            , GInfo(gInfo)
        {}

        void DeleteParts(TActorId selfId, const TVector<TPartOnMain>& parts) {
            for (const auto& part: parts) {
                if (part.HasOnMain) {
                    DeleteLocal(selfId, part.Key);
                }
            }
        }

        void DeleteLocal(TActorId selfId, const TLogoBlobID& key) {
            TLogoBlobID keyWithoutPartId(key, 0);

            TIngress ingress;
            ingress.DeleteHandoff(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, key);

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB20, VDISKP(Ctx->VCtx, "Deleting local"), (LogoBlobID, key.ToString()),
                (Ingress, ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId)));

            TlsActivationContext->Send(
                new IEventHandle(Ctx->SkeletonId, selfId, new TEvDelLogoBlobDataSyncLog(keyWithoutPartId, ingress, OrderId++)));

            ++Ctx->MonGroup.MarkedReadyToDelete();
            Ctx->MonGroup.MarkedReadyToDeleteBytes() += GInfo->GetTopology().GType.PartSize(key);
            ++RequestsSent;
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            ++Responses;
            ++Ctx->MonGroup.MarkedReadyToDeleteResponse();
            Ctx->MonGroup.MarkedReadyToDeleteWithResponseBytes() += GInfo->GetTopology().GType.PartSize(ev->Get()->Id);
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB21, VDISKP(Ctx->VCtx, "Deleted local"), (LogoBlobID, ev->Get()->Id));
        }

        bool IsDone() const {
            return Responses == RequestsSent;
        }
    };


    class TDeleter : public TActorBootstrapped<TDeleter> {
        TActorId NotifyId;
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TPartsRequester PartsRequester;
        TPartsDeleter PartsDeleter;

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  RequestState
        ///////////////////////////////////////////////////////////////////////////////////////////

        void SendRequestsToCheckPartsOnMain() {
            Become(&TThis::RequestState);

            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB22, VDISKP(Ctx->VCtx, "SendRequestsToCheckPartsOnMain"), (Parts, PartsRequester.GetPartsSize()));

            if (PartsRequester.GetPartsSize() == 0) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB23, VDISKP(Ctx->VCtx, "Nothing to request. PassAway"));
                PassAway();
                return;
            }

            PartsRequester.SendRequestsToCheckPartsOnMain(SelfId());

            Schedule(TDuration::Seconds(15), new NActors::TEvents::TEvWakeup(REQUEST_TIMEOUT_TAG)); // read timeout
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            PartsRequester.Handle(ev);
            if (PartsRequester.IsDone()) {
                DeleteLocalParts();
            }
        }

        void TimeoutRequest(NActors::TEvents::TEvWakeup::TPtr ev) {
            if (ev->Get()->Tag != REQUEST_TIMEOUT_TAG) {
                return;
            }
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB24, VDISKP(Ctx->VCtx, "SendRequestsToCheckPartsOnMain timeout"));
            DeleteLocalParts();
        }

        STRICT_STFUNC(RequestState,
            hFunc(TEvBlobStorage::TEvVGetResult, Handle)
            hFunc(NActors::TEvents::TEvWakeup, TimeoutRequest)

            hFunc(TEvVGenerationChange, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        );


        ///////////////////////////////////////////////////////////////////////////////////////////
        //  DeleteState
        ///////////////////////////////////////////////////////////////////////////////////////////

        void DeleteLocalParts() {
            Become(&TThis::DeleteState);

            if (PartsRequester.GetResult().empty()) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB25, VDISKP(Ctx->VCtx, "Nothing to delete. PassAway"));
                PassAway();
                return;
            }

            {
                ui32 partsOnMain = 0;
                for (const auto& part: PartsRequester.GetResult()) {
                    partsOnMain += part.HasOnMain;
                }
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB26, VDISKP(Ctx->VCtx, "DeleteLocalParts"), (Parts, PartsRequester.GetResult().size()), (PartsOnMain, partsOnMain));
            }

            PartsDeleter.DeleteParts(SelfId(), PartsRequester.GetResult());

            Schedule(TDuration::Seconds(15), new NActors::TEvents::TEvWakeup(DELETE_TIMEOUT_TAG)); // delete timeout
        }

        void HandleDelLogoBlobResult(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            PartsDeleter.Handle(ev);
            if (PartsDeleter.IsDone()) {
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB27, VDISKP(Ctx->VCtx, "DeleteLocalParts done"));
                PassAway();
            }
        }

        void TimeoutDelete(NActors::TEvents::TEvWakeup::TPtr ev) {
            if (ev->Get()->Tag != DELETE_TIMEOUT_TAG) {
                return;
            }
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB28, VDISKP(Ctx->VCtx, "DeleteLocalParts timeout"));
            PassAway();
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(DELETER_ID));
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(DeleteState,
            hFunc(TEvDelLogoBlobDataSyncLogResult, HandleDelLogoBlobResult)
            hFunc(NActors::TEvents::TEvWakeup, TimeoutDelete)

            hFunc(TEvVGenerationChange, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        );

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Helper functions
        ///////////////////////////////////////////////////////////////////////////////////////////

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

    public:
        TDeleter() = default;
        TDeleter(
            TActorId notifyId,
            TConstArrayRef<TLogoBlobID> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , PartsRequester(SelfId(), parts, Ctx->VCtx->ReplNodeRequestQuoter, GInfo, queueActorMapPtr, Ctx->MonGroup)
            , PartsDeleter(ctx, GInfo)
        {
        }

        void Bootstrap() {
            SendRequestsToCheckPartsOnMain();
        }
    };
}

IActor* CreateDeleterActor(
    TActorId notifyId,
    TConstArrayRef<TLogoBlobID> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TDeleter(notifyId, parts, queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
