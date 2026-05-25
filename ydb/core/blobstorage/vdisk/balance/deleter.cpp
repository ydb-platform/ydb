#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_BALANCING

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
        TVector<TLogoBlobID> Parts;
        TReplQuoter::TPtr Quoter;
        NMonitoring::TDynamicCounters::TCounterPtr QuoterThrottledCounter;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQueueActorMapPtr QueueActorMapPtr;
        NMonGroup::TBalancingGroup& MonGroup;

        TVector<TPartOnMain> Result;
        ui32 RequestsSent = 0;
        ui32 Responses = 0;
    public:

        TPartsRequester(TActorId notifyId, TVector<TLogoBlobID>&& parts, TReplQuoter::TPtr quoter, NMonitoring::TDynamicCounters::TCounterPtr quoterThrottledCounter, TIntrusivePtr<TBlobStorageGroupInfo> gInfo, TQueueActorMapPtr queueActorMapPtr, NMonGroup::TBalancingGroup& monGroup)
            : NotifyId(notifyId)
            , Parts(std::move(parts))
            , Quoter(quoter)
            , QuoterThrottledCounter(quoterThrottledCounter)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
            , MonGroup(monGroup)
            , Result(Parts.size())
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
                    msgSize,
                    0,
                    QuoterThrottledCounter
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

            YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Deleting local"),
                {"Marker", "BSVB20"},
                {"LogoBlobID", key.ToString()},
                {"Ingress", ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, keyWithoutPartId)});

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
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "Deleted local"),
                {"Marker", "BSVB21"},
                {"LogoBlobID", ev->Get()->Id});
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

            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "SendRequestsToCheckPartsOnMain"),
                {"Marker", "BSVB22"},
                {"Parts", PartsRequester.GetPartsSize()});

            if (PartsRequester.GetPartsSize() == 0) {
                YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Nothing to request. PassAway"),
                    {"Marker", "BSVB23"});
                PassAway();
                return;
            }

            PartsRequester.SendRequestsToCheckPartsOnMain(SelfId());

            Schedule(Ctx->Cfg.RequestBlobsOnMainTimeout, new NActors::TEvents::TEvWakeup(REQUEST_TIMEOUT_TAG)); // read timeout
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
            YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "CandidatesToDeleteAskFromMainBatchTimeout"),
                {"Marker", "BSVB24"});
            Ctx->MonGroup.CandidatesToDeleteAskFromMainBatchTimeout()++;
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

        ui32 CheckPartsOnMain() {
            TVector<TLogoBlobID> partsNotOnMain;
            for (const auto& part: PartsRequester.GetResult()) {
                if (!part.HasOnMain) {
                    partsNotOnMain.push_back(part.Key);
                }
            }

            if (!partsNotOnMain.empty()) {
                YDB_LOG_INFO(VDISKP(Ctx->VCtx, "Send TEvBalancingSendPartsOnMain"),
                    {"Marker", "BSVB29"});
                Send(NotifyId, new TEvBalancingSendPartsOnMain(std::move(partsNotOnMain)));
            }

            ui32 partsOnMain = PartsRequester.GetResult().size() - partsNotOnMain.size();
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "DeleteLocalParts"),
                {"Marker", "BSVB30"},
                {"Parts", PartsRequester.GetResult().size()},
                {"PartsOnMain", partsOnMain});

            return partsOnMain;
        }

        void DeleteLocalParts() {
            Become(&TThis::DeleteState);

            if (CheckPartsOnMain() == 0) {
                YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Nothing to delete. PassAway"),
                    {"Marker", "BSVB25"});
                PassAway();
                return;
            }

            PartsDeleter.DeleteParts(SelfId(), PartsRequester.GetResult());

            Schedule(Ctx->Cfg.DeleteBatchTimeout, new NActors::TEvents::TEvWakeup(DELETE_TIMEOUT_TAG)); // delete timeout
        }

        void HandleDelLogoBlobResult(TEvDelLogoBlobDataSyncLogResult::TPtr ev) {
            PartsDeleter.Handle(ev);
            if (PartsDeleter.IsDone()) {
                YDB_LOG_INFO(VDISKP(Ctx->VCtx, "DeleteLocalParts done"),
                    {"Marker", "BSVB27"});
                PassAway();
            }
        }

        void TimeoutDelete(NActors::TEvents::TEvWakeup::TPtr ev) {
            if (ev->Get()->Tag != DELETE_TIMEOUT_TAG) {
                return;
            }
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "MarkReadyBatchTimeout"),
                {"Marker", "BSVB31"});
            Ctx->MonGroup.MarkReadyBatchTimeout()++;
            PassAway();
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted());
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "TDeleter::PassAway"),
                {"Marker", "BSVB32"});
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
            TVector<TLogoBlobID>&& parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , PartsRequester(SelfId(), std::move(parts), Ctx->VCtx->ReplNodeRequestQuoter, Ctx->ReplMonGroup.ReplNodeRequestThrottledMicrosecondsPtr(), GInfo, queueActorMapPtr, Ctx->MonGroup)
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
    TVector<TLogoBlobID>&& parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TDeleter(notifyId, std::move(parts), queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
