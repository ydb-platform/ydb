#pragma once
#include "events.h"
#include "manager.h"

#include "abstract/collector.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TActor: public TActorBootstrapped<TActor> {
private:
    const ui64 TabletId;
    const NActors::TActorId Parent;
    std::shared_ptr<TLocalManager> Manager;

    std::shared_ptr<IAccessorCallback> AccessorsCallback;

    void StartStopping() {
        PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        Manager->RegisterController(ev->Get()->ExtractController(), ev->Get()->IsUpdate());
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        Manager->UnregisterController(ev->Get()->GetPathId());
    }
    void Handle(TEvAddPortion::TPtr& ev) {
        for (auto&& a : ev->Get()->ExtractAccessors()) {
            Manager->AddPortion(std::move(a));
        }
    }
    void Handle(TEvRemovePortion::TPtr& ev) {
        Manager->RemovePortion(ev->Get()->GetPortion());
    }
    void Handle(TEvAskServiceDataAccessors::TPtr& ev);
    
public:
    TActor(const ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent) {
        Y_UNUSED(TabletId);
    }
    ~TActor() = default;

    void Bootstrap();

    STFUNC(StateWait) {
        const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("self_id", SelfId())("tablet_id", TabletId)("parent", Parent);
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, StartStopping);
            hFunc(TEvRegisterController, Handle);
            hFunc(TEvUnregisterController, Handle);
            hFunc(TEvAskServiceDataAccessors, Handle);
            hFunc(TEvRemovePortion, Handle);
            hFunc(TEvAddPortion, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
