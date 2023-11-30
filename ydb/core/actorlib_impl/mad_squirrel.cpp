#include "mad_squirrel.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors {

class TMadSquirrel : public TActor<TMadSquirrel> {
    TAutoPtr<IEventHandle> AfterRegister(const TActorId &self, const TActorId &) override {
        return new IEventHandle(self, self, new TEvents::TEvWakeup());
    }

public:
    static constexpr auto ActorActivityType() {
        return IActor::EActivityType::ACTORLIB_COMMON;
    }

    TMadSquirrel()
        : TActor(&TThis::StateFunc)
    {}

    STFUNC(StateFunc) {
        Y_UNUSED(ev);
        Send(SelfId(), new TEvents::TEvWakeup());
    }
};

IActor* CreateMadSquirrel() {
    return new TMadSquirrel();
}

}
