#include "actor.h"

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

class TCookie: public TThrRefBase {
private:
    YDB_READONLY(ui64, TaskIdentifier, 0);
public:
    TCookie(const ui64 id)
        : TaskIdentifier(id)
    {

    }
};

void TActor::DoReplyAllocated(const ui64 internalTaskId, const ui64 rbTaskId) {
    auto it = Tasks.find(internalTaskId);
    Y_ABORT_UNLESS(it != Tasks.end());
    auto task = it->second;
    Tasks.erase(it);
    task->GetContext().GetCounters()->OnReply(task->GetMemoryAllocation());
    if (Aborted) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "result_resources_on_abort")("task_id", rbTaskId)("task", task->DebugString());
        std::make_unique<TResourcesGuard>(rbTaskId, task->GetExternalTaskId(), *task, SelfId(), task->GetContext());
        if (Tasks.empty()) {
            PassAway();
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "result_resources")("task_id", rbTaskId)("task", task->DebugString());
        task->OnAllocationSuccess(rbTaskId, SelfId());
    }
}

void TActor::Handle(TEvStartTask::TPtr& ev) {
    Y_ABORT_UNLESS(!Aborted);
    auto task = ev->Get()->GetTask();
    Y_ABORT_UNLESS(task);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ask_resources")("task", task->DebugString());
    Tasks.emplace(++Counter, task);
    if (!task->GetCPUAllocation() && !task->GetMemoryAllocation()) {
        DoReplyAllocated(Counter, 0);
    } else {
        Send(NKikimr::NResourceBroker::MakeResourceBrokerID(), new NKikimr::NResourceBroker::TEvResourceBroker::TEvSubmitTask(
            task->GetExternalTaskId(),
            {{task->GetCPUAllocation(), task->GetMemoryAllocation()}},
            task->GetType(),
            task->GetPriority(),
            new TCookie(Counter)
        ));
    }
    task->GetContext().GetCounters()->OnRequest(task->GetMemoryAllocation());
}

void TActor::Handle(NKikimr::NResourceBroker::TEvResourceBroker::TEvResourceAllocated::TPtr& ev) {
    DoReplyAllocated(((TCookie*)ev->Get()->Cookie.Get())->GetTaskIdentifier(), ev->Get()->TaskId);
}

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent)
{

}

TActor::~TActor() {
    Y_ABORT_UNLESS(!NActors::TlsActivationContext || Tasks.empty());
}

}
