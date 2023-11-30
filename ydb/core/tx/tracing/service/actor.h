#pragma once
#include "global.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NTracing {

class TRegularTracesCleanerActor: public NActors::TActorBootstrapped<TRegularTracesCleanerActor> {
private:
    void Handle(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
        Singleton<TTracing>()->Clean();
        Schedule(TDuration::Seconds(5), new NActors::TEvents::TEvWakeup);
    }
public:
    TRegularTracesCleanerActor() {

    }

    void Bootstrap() {
        Schedule(TDuration::Seconds(5), new NActors::TEvents::TEvWakeup);
        Become(&TRegularTracesCleanerActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};

}
