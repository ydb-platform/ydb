#include "balancing_actor.h"
#include "defs.h"


namespace NKikimr {

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
    public:
        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        );

        TBalancingActor(std::shared_ptr<TBalancingCtx> &ctx)
            : TActorBootstrapped<TBalancingActor>()
            , Ctx(ctx)
        {
        }
    };

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new TBalancingActor(ctx);
    }
}
