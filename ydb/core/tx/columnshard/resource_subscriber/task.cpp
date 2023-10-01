#include "task.h"
#include "events.h"
#include <ydb/core/tablet/resource_broker.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

void ITask::OnAllocationSuccess(const ui64 taskId, const NActors::TActorId& senderId) {
    DoOnAllocationSuccess(std::make_shared<TResourcesGuard>(taskId, *this, senderId, Context));
}

void ITask::Start(const NActors::TActorId& actorId, const std::shared_ptr<ITask>& task) {
    NActors::TActorContext::AsActorContext().Send(actorId, std::make_unique<TEvStartTask>(task));
}

TResourcesGuard::~TResourcesGuard() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "free_resources")("task_id", TaskId)("mem", Memory)("cpu", Cpu);
    auto ev = std::make_unique<IEventHandle>(NKikimr::NResourceBroker::MakeResourceBrokerID(), Sender, new NKikimr::NResourceBroker::TEvResourceBroker::TEvFinishTask(TaskId));
    NActors::TActorContext::AsActorContext().Send(std::move(ev));
    Context.GetCounters()->GetBytesAllocated()->Remove(Memory);
}

TResourcesGuard::TResourcesGuard(const ui64 taskId, const ITask& task, const NActors::TActorId& sender, const TTaskContext& context)
    : TaskId(taskId)
    , Sender(sender)
    , Memory(task.GetMemoryAllocation())
    , Cpu(task.GetCPUAllocation())
    , Context(context)
{
    Context.GetCounters()->GetBytesAllocated()->Add(Memory);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "allocate_resources")("task_id", TaskId)("mem", Memory)("cpu", Cpu);
}

}
