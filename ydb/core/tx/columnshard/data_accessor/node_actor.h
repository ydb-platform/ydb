#pragma once
#include "events.h"
#include "manager.h"

#include "abstract/collector.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TNodeActor: public TActorBootstrapped<TNodeActor> {
private:
    std::shared_ptr<TLocalManager> Manager;

    std::shared_ptr<IAccessorCallback> AccessorsCallback;

    void StartStopping() {
        PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        Manager->RegisterController(ev->Get()->ExtractController(), ev->Get()->IsUpdate());
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        Manager->UnregisterController(ev->Get()->GetTabletId(), ev->Get()->GetPathId());
    }
    void Handle(TEvAddPortion::TPtr& ev) {
        for (auto&& a : ev->Get()->ExtractAccessors()) {
            Manager->AddPortion(ev->Get()->GetTabletId(), std::move(a));
        }
    }
    void Handle(TEvRemovePortion::TPtr& ev) {
        Manager->RemovePortion(ev->Get()->GetTabletId(), ev->Get()->GetPortion());
    }
    void Handle(TEvAskServiceDataAccessors::TPtr& ev);

public:

    static inline TActorId MakeActorId(ui32 nodeId) {
        char x[12] = {'s', 'h', 'a', 'r', 'e',
            'd', 'm', 'e', 't', 'a', 'd', 't'};
        return TActorId(nodeId, TStringBuf(x, 12));
    }

    static NActors::IActor* CreateActor();

    TNodeActor() = default;
    ~TNodeActor() = default;

    void Bootstrap();

    STFUNC(StateWait) {
        const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("self_id", SelfId());
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
