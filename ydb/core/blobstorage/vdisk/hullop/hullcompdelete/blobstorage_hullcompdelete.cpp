#include "blobstorage_hullcompdelete.h"

namespace NKikimr {

    class TDelayedCompactionDeleterActor : public TActor<TDelayedCompactionDeleterActor> {
        // pointer to database general data; actually we need only HugeKeeperID from that data
        const TActorId HugeKeeperId;
        const TActorId SkeletonId;
        const TPDiskCtxPtr PDiskCtx;
        const TVDiskContextPtr VCtx;

        // pointer to shared deleter state, it is primarily created in TLevelIndex
        const TIntrusivePtr<TDelayedCompactionDeleterInfo> Info;

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_DELAYED_HUGE_BLOB_DELETER;
        }

        TDelayedCompactionDeleterActor(const TActorId hugeKeeperId, const TActorId skeletonId, TPDiskCtxPtr pdiskCtx,
                TVDiskContextPtr vctx, TIntrusivePtr<TDelayedCompactionDeleterInfo> info)
            : TActor(&TDelayedCompactionDeleterActor::StateFunc)
            , HugeKeeperId(hugeKeeperId)
            , SkeletonId(skeletonId)
            , PDiskCtx(pdiskCtx)
            , VCtx(std::move(vctx))
            , Info(std::move(info))
        {}

        void Handle(TEvHullReleaseSnapshot::TPtr& ev, const TActorContext& ctx) {
            Info->ReleaseSnapshot(ev->Get()->Cookie, ctx, HugeKeeperId, SkeletonId, PDiskCtx, VCtx);
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

    IActor *CreateDelayedCompactionDeleterActor(const TActorId hugeKeeperId, const TActorId skeletonId,
            TPDiskCtxPtr pdiskCtx, TVDiskContextPtr vctx, TIntrusivePtr<TDelayedCompactionDeleterInfo> info) {
        return new TDelayedCompactionDeleterActor(hugeKeeperId, skeletonId, std::move(pdiskCtx), std::move(vctx),
            std::move(info));
    }

} // NKikimr
