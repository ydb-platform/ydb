#include "dq_task_execution.h"

#include <util/generic/scope.h>

namespace NYql::NDq {

TDqTaskExecution::TDqTaskExecution(TIntrusivePtr<IDqTaskRunner> runner, TDqMemoryQuota* memoryQuota,
    std::shared_ptr<IDqTaskScheduler> scheduler, std::shared_ptr<TTaskSchedulingHandle> task,
    IDqSchedulableWorkFactoryPtr workFactory, NActors::TActorSystem* actorSystem, NActors::TActorId endpoint)
    : Runner(std::move(runner)), MemoryQuota(memoryQuota), Scheduler(std::move(scheduler)), Task(std::move(task)),
      Work(workFactory ? workFactory->CreateSchedulableWork() : nullptr), ActorSystem(actorSystem), Endpoint(endpoint) {}

bool TDqTaskExecution::TryAcquire() {
    bool expected = true;
    return Available->compare_exchange_strong(expected, false, std::memory_order_acquire);
}

bool TDqTaskExecution::TryAcquireEndpoint() {
    // Register before testing the lease, so release cannot race past parking.
    EndpointWaiting.store(true, std::memory_order_release);
    if (TryAcquire()) {
        EndpointWaiting.store(false, std::memory_order_release);
        return true;
    }
    return false;
}

void TDqTaskExecution::Release() {
    Available->store(true, std::memory_order_release);
    if (EndpointWaiting.exchange(false, std::memory_order_acq_rel)) {
        ActorSystem->Send(Endpoint, new TEvTaskExecutionAvailable());
    }
    Scheduler->NotifyExecutionAvailable();
}

void TDqTaskExecution::Retire(bool completed) {
    Task->Cancel(completed);
    Runner.Reset();
    if (WaitingForQuota) {
        Work->StopExecution();
        WaitingForQuota = false;
    }
    Work.reset();
}

TTaskSliceResult TDqTaskExecution::RunSlice(const TTaskSliceBudget& budget) {
    TTaskSliceResult result;
    ui64 cpuUs = budget.CpuUs;
    if (Work) {
        Scheduler->SetQueryWeight(Task->Query, Work->GetSchedulingWeight());
        const auto grant = Work->TryAcquireSlice(TMonotonic::Now(), cpuUs);
        if (grant.RetryAfter) {
            // Preserve throttle accounting across the queued wait. No CPU
            // usage slot is held. Retry or retirement balances this state.
            WaitingForQuota = true;
            result.Progress.QuotaRejected = true;
            result.RetryAfter = grant.RetryAfter;
            return result;
        }
        WaitingForQuota = false;
        cpuUs = grant.CpuUs;
    }
    Y_DEFER { if (Work) { Work->StopExecution(); } };
    auto guard = Runner->BindAllocator();
    TDqOperatorMemoryQuotaScope quotaScope(MemoryQuota ? MemoryQuota->GetOperatorQuota() : nullptr);
    Runner->SetExecutionBudget(cpuUs, budget.Deadline);
    try {
        result.Status = static_cast<ui32>(Runner->Run());
    } catch (...) {
        result.Error = std::current_exception();
    }
    result.Progress = Runner->LastRunProgress;
    return result;
}

void TDqTaskExecution::OnSliceFinished(TTaskSliceResult result) {
    ActorSystem->Send(Endpoint, new TEvTaskSliceFinished(shared_from_this(), std::move(result)));
}

} // namespace NYql::NDq
