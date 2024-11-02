#pragma once
#include "controller.h"
#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TActor: public TActorBootstrapped<TActor> {
private:
    const ui64 TabletId;
    const NActors::TActorId Parent;
    THashMap<ui64, std::shared_ptr<IGranuleDataAccessor>> Controllers;

    void StartStopping() {
        PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        AFL_VERIFY(Controllers.emplace(ev->Get()->GetPathId(), ev->Get()->GetAccessor()).second);
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        AFL_VERIFY(Controllers.erase(ev->Get()->GetPathId()));
    }
    void Handle(TEvAskDataAccessors::TPtr& ev);

public:
    TActor(const ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent) {
    }
    ~TActor() = default;

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", Parent)(
            "ev_type", ev->GetTypeName()));
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, StartStopping);
            hFunc(TEvRegisterController, Handle);
            hFunc(TEvUnregisterController, Handle);
            hFunc(TEvAskDataAccessors, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
