#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/protos/services_common.pb.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/actors/core/events.h>

namespace NActors {

class TDestructActor: public TActor<TDestructActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::INTERCONNECT_DESTRUCT_ACTOR;
    }

    TDestructActor() noexcept
        : TActor(&TThis::WorkingState)
    {}


private:
    STATEFN(WorkingState) {
        /* Destroy event and eventhandle */
        ev.Reset();
    }
};

TActorId GetDestructActorID() noexcept {
    return TActorId(0, "destructor");
}

}
