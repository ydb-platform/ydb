#pragma once
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NPersQueue {

// Fake object to hold actor id
class TFakeActor: public NActors::TActor<TFakeActor> {
public:
    TFakeActor()
        : TActor<TFakeActor>(&TFakeActor::StateFunc)
    {
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
        }
    }
};

} // namespace NPersQueue
