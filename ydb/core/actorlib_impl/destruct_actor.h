#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/events.h>

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
