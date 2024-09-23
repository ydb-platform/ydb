#include "task.h"
#include "events.h"
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

void ITask::OnAllocationSuccess(const ui64 taskId, const NActors::TActorId& senderId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "resource_allocated")("external_task_id", ExternalTaskId)("mem", MemoryAllocation)("cpu", CPUAllocation);
    DoOnAllocationSuccess(std::make_shared<TResourcesGuard>(taskId, ExternalTaskId, *this, senderId, Context));
}

void ITask::StartResourceSubscription(const NActors::TActorId& actorId, const std::shared_ptr<ITask>& task) {
    NActors::TActorContext::AsActorContext().Send(actorId, std::make_unique<TEvStartTask>(task));
}

TResourcesGuard::~TResourcesGuard() {
    if (!NActors::TlsActivationContext) {
        return;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "free_resources")("task_id", TaskId)("external_task_id", ExternalTaskId)("mem", Memory)("cpu", Cpu);
    if (TaskId) {
        auto ev = std::make_unique<IEventHandle>(NKikimr::NResourceBroker::MakeResourceBrokerID(), Sender, new NKikimr::NResourceBroker::TEvResourceBroker::TEvFinishTask(TaskId));
        NActors::TActorContext::AsActorContext().Send(std::move(ev));
        Context.GetCounters()->GetBytesAllocated()->Remove(Memory);
    }
}

TResourcesGuard::TResourcesGuard(const ui64 taskId, const TString& externalTaskId, const ITask& task, const NActors::TActorId& sender, const TTaskContext& context)
    : TaskId(taskId)
    , ExternalTaskId(externalTaskId)
    , Sender(sender)
    , Memory(task.GetMemoryAllocation())
    , Cpu(task.GetCPUAllocation())
    , Context(context)
    , Priority(task.GetPriority())
{
    AFL_VERIFY(taskId || (!Memory && !Cpu));
    Context.GetCounters()->GetBytesAllocated()->Add(Memory);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "allocate_resources")("external_task_id", ExternalTaskId)("task_id", TaskId)("mem", Memory)("cpu", Cpu);
}

void TResourcesGuard::Update(const ui64 memNew) {
    if (!TaskId) {
        return;
    }
    AFL_VERIFY(Memory);
    Context.GetCounters()->GetBytesAllocated()->Remove(Memory);
    AFL_VERIFY(NActors::TlsActivationContext);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "update_resources")("task_id", TaskId)("external_task_id", ExternalTaskId)("mem", memNew)("cpu", Cpu)("mem_old", Memory);
    Memory = memNew;
    auto ev = std::make_unique<IEventHandle>(NKikimr::NResourceBroker::MakeResourceBrokerID(), Sender, new NKikimr::NResourceBroker::TEvResourceBroker::TEvUpdateTask(TaskId, {{Cpu, Memory}},
        Context.GetTypeName(), Priority));
    NActors::TActorContext::AsActorContext().Send(std::move(ev));
    Context.GetCounters()->GetBytesAllocated()->Add(Memory);
}

}
