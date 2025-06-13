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

    std::shared_ptr<Foo> AccessorsCallback;

    void StartStopping() {
        AccessorsCallback = std::make_shared<TActorAccessorsCallback>(SelfId());
        Manager = std::make_shared<TLocalManager>(AccessorsCallback);
        // PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        auto controller = ev->Get()->ExtractController();
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "Reg")("path", controller->GetPathId())("Sender", ev->Get()->GetOwner());
        Manager->RegisterController(move(controller), ev->Get()->GetIsUpdateFlag(), ev->Get()->GetOwner());
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "Unreg")("Sender", ev->Get()->GetOwner());
        Manager->UnregisterController(ev->Get()->GetPathId(), ev->Get()->GetOwner());
    }
    void Handle(TEvAddPortion::TPtr& ev) {
        for (auto&& a : ev->Get()->ExtractAccessors()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "Add")("path", a.GetPortionInfo().GetPathId())("Sender", ev->Get()->GetOwner());
            Manager->AddPortion(std::move(a), ev->Get()->GetOwner());
        }
    }
    void Handle(TEvRemovePortion::TPtr& ev) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "Remove")("Sender", ev->Get()->GetOwner());
        Manager->RemovePortion(ev->Get()->GetPortion(), ev->Get()->GetOwner());
    }
    void Handle(TEvAskServiceDataAccessors::TPtr& ev);
    void Handle(TEvClearCache::TPtr& ev) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("IURII", "CLEAR")("Sender", ev->Get()->GetOwner());
        Manager->ClearCache(ev->Get()->GetOwner());
    }

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
            hFunc(TEvClearCache, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
