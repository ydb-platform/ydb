#pragma once
#include "controller.h"
#include "events.h"
#include "manager.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TActor: public TActorBootstrapped<TActor> {
private:
    const ui64 TabletId;
    const NActors::TActorId Parent;
    TLocalManager Manager;

    void StartStopping() {
        PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        Manager.RegisterController(ev->Get()->ExtractController());
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        Manager.UnregisterController(ev->Get()->GetPathId());
    }
    void Handle(TEvAddPortion::TPtr& ev) {
        Manager.AddPortion(ev->Get()->ExtractAccessor());
    }
    void Handle(TEvRemovePortion::TPtr& ev) {
        Manager.RemovePortion(ev->Get()->GetPortion());
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
            hFunc(TEvRemovePortion, Handle);
            hFunc(TEvAddPortion, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
