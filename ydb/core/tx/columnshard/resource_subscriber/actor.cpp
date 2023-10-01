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

void TActor::Handle(TEvStartTask::TPtr& ev) {
    auto task = ev->Get()->GetTask();
    Y_VERIFY(task);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ask_resources")("task", task->DebugString());
    Tasks.emplace(++Counter, task);
    Send(NKikimr::NResourceBroker::MakeResourceBrokerID(), new NKikimr::NResourceBroker::TEvResourceBroker::TEvSubmitTask(
        task->GetName(),
        {{task->GetCPUAllocation(), task->GetMemoryAllocation()}},
        task->GetType(),
        task->GetPriority(),
        new TCookie(Counter)
    ));
    task->GetContext().GetCounters()->OnRequest(task->GetMemoryAllocation());
}

void TActor::Handle(NKikimr::NResourceBroker::TEvResourceBroker::TEvResourceAllocated::TPtr& ev) {
    auto it = Tasks.find(((TCookie*)ev->Get()->Cookie.Get())->GetTaskIdentifier());
    Y_VERIFY(it != Tasks.end());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "result_resources")("task_id", ev->Get()->TaskId)("task", it->second->DebugString());
    it->second->OnAllocationSuccess(ev->Get()->TaskId, SelfId());
    Tasks.erase(it);
    it->second->GetContext().GetCounters()->OnReply(it->second->GetMemoryAllocation());
}

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent)
{

}

}
