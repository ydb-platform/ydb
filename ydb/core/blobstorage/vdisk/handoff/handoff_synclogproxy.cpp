#include "handoff_synclogproxy.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Handoff Sync Log Proxy
    // It is used to write handoff delete command to recovery log and sync log
    ////////////////////////////////////////////////////////////////////////////
    class THandoffSyncLogProxy : public TActor<THandoffSyncLogProxy> {

        struct TItem {
            ui64 OrderId;
        };
        typedef TDeque<TItem> TQueueType;

        const TActorId SkeletonId;
        const TActorId NotifyId;
        ui64 OrderId = 0;
        bool Finished = false;
        TQueueType Queue = {};

        void CheckAndFinish(const TActorContext &ctx) {
            if (Finished && Queue.empty()) {
                ctx.Send(NotifyId, new TEvHandoffSyncLogFinished(true));
                Die(ctx);
            }
        }

        void Handle(TEvHandoffSyncLogDel::TPtr &ev, const TActorContext &ctx) {
            auto msg = ev->Get();

            if (msg->Finished) {
                Finished = true;
                CheckAndFinish(ctx);
            } else {
                ctx.Send(SkeletonId, new TEvDelLogoBlobDataSyncLog(msg->Id, msg->Ingress, OrderId));
                // put to deque (delayed queue :)
                Queue.push_back(TItem {OrderId});
                ++OrderId;
            }
        }

        void Handle(TEvDelLogoBlobDataSyncLogResult::TPtr &ev, const TActorContext &ctx) {
            auto msg = ev->Get();
            Y_VERIFY(!Queue.empty());
            const TItem &item = Queue.front();
            Y_VERIFY(msg->OrderId == item.OrderId);
            Queue.pop_front();

            CheckAndFinish(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvHandoffSyncLogDel, Handle)
            HFunc(TEvDelLogoBlobDataSyncLogResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HANDOFF_SYNCLOG_PROXY;
        }

        THandoffSyncLogProxy(const TActorId &skeletonId,
                             const TActorId &notifyId)
            : TActor<THandoffSyncLogProxy>(&TThis::StateFunc)
            , SkeletonId(skeletonId)
            , NotifyId(notifyId)
        {
        }
    };

    IActor* CreateHandoffSyncLogProxy(const TActorId &skeletonId, const TActorId &notifyId) {
        return new THandoffSyncLogProxy(skeletonId, notifyId);
    }

} // NKikimr
