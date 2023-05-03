#include "mad_squirrel.h"
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NActors {

class TMadSquirrel : public TActor<TMadSquirrel> {
    TAutoPtr<IEventHandle> AfterRegister(const TActorId &self, const TActorId &) override {
        return new IEventHandle(self, self, new TEvents::TEvWakeup());
    }

public:
    static constexpr auto ActorActivityType() {
        return ACTORLIB_COMMON;
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
