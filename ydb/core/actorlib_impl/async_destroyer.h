#pragma once
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/generic/ptr.h>

namespace NKikimr {

// Helps to destroy heavy objects
template <class TVictim>
class TAsyncDestroyer : public NActors::TActorBootstrapped<TAsyncDestroyer<TVictim>> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ASYNC_DESTROYER;
    }

    explicit TAsyncDestroyer(TAutoPtr<TVictim> victim)
        : Victim(victim)
    {}

    void Bootstrap(const NActors::TActorContext &ctx) {
        Victim.Destroy();
        this->Die(ctx);
    }

private:
    TAutoPtr<TVictim> Victim;
};

template <class TVictim>
void AsyncDestroy(TAutoPtr<TVictim> victim, const NActors::TActorContext &ctx, ui32 poolId = Max<ui32>()) {
    if (victim)
        ctx.Register(new TAsyncDestroyer<TVictim>(victim), NActors::TMailboxType::HTSwap, poolId);
}

}
