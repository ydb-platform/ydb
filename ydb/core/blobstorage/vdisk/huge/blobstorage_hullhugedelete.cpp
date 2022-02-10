#include "blobstorage_hullhugedelete.h"

namespace NKikimr {

    class TDelayedHugeBlobDeleterActor : public TActor<TDelayedHugeBlobDeleterActor> {
        // pointer to database general data; actually we need only HugeKeeperID from that data
        const TActorId HugeKeeperId;

        // pointer to shared deleter state, it is primarily created in TLevelIndex
        const TIntrusivePtr<TDelayedHugeBlobDeleterInfo> Info;

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_DELAYED_HUGE_BLOB_DELETER;
        }

        TDelayedHugeBlobDeleterActor(const TActorId &hugeKeeperId,
                TIntrusivePtr<TDelayedHugeBlobDeleterInfo> info)
            : TActor(&TDelayedHugeBlobDeleterActor::StateFunc)
            , HugeKeeperId(hugeKeeperId) 
            , Info(std::move(info))
        {}

        void Handle(TEvHullReleaseSnapshot::TPtr& ev, const TActorContext& ctx) {
            Info->ReleaseSnapshot(ev->Get()->Cookie, ctx, HugeKeeperId); 
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) { 
            Y_UNUSED(ev); 
            Die(ctx); 
        } 
 
        STRICT_STFUNC(StateFunc,
            HFunc(TEvHullReleaseSnapshot, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )
    };

    IActor *CreateDelayedHugeBlobDeleterActor(const TActorId &hugeKeeperId,
            TIntrusivePtr<TDelayedHugeBlobDeleterInfo> info) { 
        return new TDelayedHugeBlobDeleterActor(hugeKeeperId, std::move(info)); 
    }

} // NKikimr
