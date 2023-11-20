#pragma once

#include "task.h"
#include "events.h"
#include <ydb/core/tablet/resource_broker.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

class TActor: public TActorBootstrapped<TActor> {
private:
    ui64 TabletId;
    NActors::TActorId Parent;
    THashMap<ui64, std::shared_ptr<ITask>> Tasks;
    ui64 Counter = 0;
    bool Aborted = false;

    void StartStopping() {
        Aborted = true;
        if (Tasks.empty()) {
            PassAway();
        }
    }

public:
    static TAtomicCounter WaitingBlobsCount;
    TActor(ui64 tabletId, const TActorId& parent);
    ~TActor();

    void Handle(TEvStartTask::TPtr& ev);
    void Handle(NKikimr::NResourceBroker::TEvResourceBroker::TEvResourceAllocated::TPtr& ev);

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", Parent));
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, StartStopping);
            hFunc(TEvStartTask, Handle);
            hFunc(NKikimr::NResourceBroker::TEvResourceBroker::TEvResourceAllocated, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}
