#pragma once

#include "mon_events.h"
#include "monitoring.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/json/json_value.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NSchemeBoard {

template <typename TDerived>
class TMonitorableActor: public TActorBootstrapped<TDerived> {
    void MonRegister() {
        this->Send(MakeSchemeBoardMonitoringId(), new TSchemeBoardMonEvents::TEvRegister(
            TDerived::ActorActivityType(), this->MonAttributes()));
    }

    void MonUnregister() {
        this->Send(MakeSchemeBoardMonitoringId(), new TSchemeBoardMonEvents::TEvUnregister());
    }

protected:
    virtual NJson::TJsonMap MonAttributes() const {
        return {};
    }

    static NJson::TJsonMap PrintActorIdAttr(NKikimrServices::TActivity::EType activityType, const TActorId& actorId) {
        return NJson::TJsonMap({
            {"@type", "ACTOR_ID"},
            {"ActivityType", NKikimrServices::TActivity::EType_Name(activityType)},
            {"ActorId", Sprintf("%" PRIu64 ":%" PRIu64, actorId.RawX1(), actorId.RawX2())},
        });
    }

public:
    void Bootstrap() {
        MonRegister();
    }

    void PassAway() override {
        MonUnregister();
        IActor::PassAway();
    }
};

} // NSchemeBoard
} // NKikimr
