#include "dq_task_execution.h"

#include <util/generic/scope.h>
#include <util/system/env.h>

namespace NYql::NDq {

TDqTaskExecution::TDqTaskExecution(TIntrusivePtr<IDqTaskRunner> runner, TDqMemoryQuota* memoryQuota,
    std::shared_ptr<IDqTaskScheduler> scheduler, std::shared_ptr<TTaskSchedulingHandle> task,
    IDqSchedulableWorkFactoryPtr workFactory, NActors::TActorSystem* actorSystem, NActors::TActorId endpoint)
    : Runner(std::move(runner)), MemoryQuota(memoryQuota), Scheduler(std::move(scheduler)), Task(std::move(task)),
      Work(workFactory ? workFactory->CreateSchedulableWork() : nullptr), ActorSystem(actorSystem), Endpoint(endpoint) {}

bool TDqTaskExecution::TryAcquire() {
    return Available->TryAcquire();
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
    const bool queueWaiting = Available->Release();
    if (EndpointWaiting.exchange(false, std::memory_order_acq_rel)) {
        ActorSystem->Send(Endpoint, new TEvTaskExecutionAvailable());
    }
    if (queueWaiting) {
        Scheduler->NotifyExecutionAvailable();
    }
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
    static const bool finishOnce = GetEnv("YDB_DQ_TASK_FINISH_ONCE") == "1";
    if (finishOnce && Runner->LastFetchStatus == ERunStatus::Finished) {
        // The previous Run has finished every output. The endpoint still
        // processes channel acknowledgements and retires the task under its lease.
        result.Status = static_cast<ui32>(ERunStatus::Finished);
        return result;
    }
    auto guard = Runner->BindAllocator();
    TDqOperatorMemoryQuotaScope quotaScope(MemoryQuota ? MemoryQuota->GetOperatorQuota() : nullptr);
    Runner->SetExecutionBudget(cpuUs, budget.Deadline);
    try {
        result.Status = static_cast<ui32>(Runner->Run());
    } catch (...) {
        result.Error = std::current_exception();
    }
    result.Progress = Runner->LastRunProgress;
    // Still under the lease: decide whether the endpoint has anything to do for this slice.
    // Finish, errors, quota and changed output/peer state (the endpoint clears the flag on
    // every incoming event) keep going through the endpoint; so does every N-th slice, for
    // periodic stats and memory shrinking.
    constexpr ui32 maxStreak = 64;
    if (InlineAllowed.load(std::memory_order_acquire) && !result.Error
        && !result.Progress.QuotaRejected && !result.RetryAfter
        && (result.Status == static_cast<ui32>(ERunStatus::PendingInput) || result.Status == static_cast<ui32>(ERunStatus::PendingOutput))
        && InlineEpoch && InlineEpoch->load(std::memory_order_acquire) == InlineCheckedEpoch
        && InlineStreak < maxStreak)
    {
        ++InlineStreak;
        result.CompleteInline = true;
    } else {
        InlineStreak = 0;
        InlineAllowed.store(false, std::memory_order_release);
    }
    return result;
}

bool TDqTaskExecution::RefreshInlinePermissionEnabled() {
    static const bool enabled = GetEnv("YDB_DQ_TASK_REFRESH_INLINE") == "1";
    return enabled;
}

void TDqTaskExecution::AllowInlineCompletion(std::shared_ptr<const std::atomic<ui64>> epoch, ui64 checkedEpoch) {
    InlineEpoch = std::move(epoch);
    InlineCheckedEpoch = checkedEpoch;
    InlineAllowed.store(true, std::memory_order_release);
}

void TDqTaskExecution::OnSliceFinished(TTaskSliceResult result) {
    if (result.CompleteInline) {
        // Same as the endpoint's CompleteWorkerSlice for a non-final slice with nothing
        // to drain: only the scheduling handle has to learn the outcome. A concurrent
        // retirement cancels the handle first, which makes Complete a no-op.
        Task->Complete(result.Progress);
        return;
    }
    ActorSystem->Send(Endpoint, new TEvTaskSliceFinished(shared_from_this(), std::move(result)));
}

} // namespace NYql::NDq
