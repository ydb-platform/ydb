#pragma once

#include "events.h"
#include "task.h"

#include <ydb/core/tablet/resource_broker.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/struct_log/log_stack.h>

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

    void DoReplyAllocated(const ui64 internalTaskId, const ui64 rbTaskId);

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
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
            {"tabletId", TabletId},
            {"parent", Parent},
            {"evType", ev->GetTypeName()});
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, StartStopping);
            hFunc(TEvStartTask, Handle);
            hFunc(NKikimr::NResourceBroker::TEvResourceBroker::TEvResourceAllocated, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NResourceBroker::NSubscribe
