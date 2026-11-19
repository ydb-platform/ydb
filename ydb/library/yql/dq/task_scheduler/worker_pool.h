#pragma once

#include "task_scheduler.h"
#include <util/generic/hash_set.h>

#include <atomic>
#include <deque>
#include <exception>

namespace NYql::NDq {

struct TTaskSliceBudget {
    ui64 CpuUs = 0;
    TMonotonic Deadline;
};

struct TTaskSliceResult {
    TTaskRunProgress Progress;
    ui32 Status = 0; // interpreted by the execution engine, not the queue
    std::exception_ptr Error;
    std::optional<TDuration> RetryAfter;
    // The slice needs no endpoint work,
    // OnSliceFinished completes the scheduling handle on the worker itself.
    bool CompleteInline = false;
};

// A queue scanning a ready task arms a notification if its lease is busy.
// The same atomic records availability and interest, so a release either
// sees the interest or the scan sees the newly available lease.
class TTaskExecutionLease {
public:
    bool IsAvailable() const noexcept {
        return State.load(std::memory_order_acquire) == EState::Available;
    }

    bool IsAvailableOrSubscribe() const noexcept {
        auto expected = State.load(std::memory_order_acquire);
        if (expected == EState::Busy
            && State.compare_exchange_strong(expected, EState::BusyWithWaiter, std::memory_order_acq_rel))
        {
            return false;
        }
        return expected == EState::Available;
    }

    bool TryAcquire() noexcept {
        auto expected = EState::Available;
        return State.compare_exchange_strong(expected, EState::Busy, std::memory_order_acquire);
    }

    // Returns whether to notify the queue.
    bool Release() noexcept {
        return State.exchange(EState::Available, std::memory_order_release) == EState::BusyWithWaiter;
    }

private:
    enum class EState : ui8 { Available, Busy, BusyWithWaiter };
    mutable std::atomic<EState> State = EState::Available;
};

// Implementations own task state. No methods of a task's endpoint actor are
// invoked by a worker. RunSlice and endpoint state access share this lease.
class IDqTaskExecution {
public:
    virtual ~IDqTaskExecution() = default;
    virtual std::shared_ptr<const TTaskExecutionLease> GetAvailability() const = 0;
    virtual bool IsAvailable() const = 0;
    virtual bool TryAcquire() = 0;
    virtual void Release() = 0;
    virtual TTaskSliceResult RunSlice(const TTaskSliceBudget& budget) = 0;
    // Called after releasing the lease. May complete the handle immediately, or
    // deliver the result to the endpoint for output/control processing first.
    virtual void OnSliceFinished(TTaskSliceResult result) = 0;
};

// A hint for the next selection of the worker that produced these data.
struct TLocalConsumerHint {
    NActors::TActorId Query, Consumer;
    ui64 ReadyRevision = 0;
    ui64 ProducerTask = 0;
    ui64 Channel = 0;
};

// Capture a synchronous local input notification during a worker slice. No
// channel or task state is retained after the slice, just the ready revision.
class TLocalConsumerScope {
public:
    TLocalConsumerScope(TLocalConsumerHint& hint, NActors::TActorId query, ui64 producerTask);
    ~TLocalConsumerScope();
    TLocalConsumerScope(const TLocalConsumerScope&) = delete;
    TLocalConsumerScope& operator=(const TLocalConsumerScope&) = delete;

    static bool Captures(NActors::TActorId query);
    static void Record(NActors::TActorId actor, ui64 readyRevision, ui64 channel);

private:
    TLocalConsumerHint* const Previous;
};

// Concurrent node-local metadata. No queue lock is held during task execution.
class TTaskWorkPool : public std::enable_shared_from_this<TTaskWorkPool> {
public:
    struct TClaim {
        std::shared_ptr<TTaskSchedulingHandle> Task;
        std::shared_ptr<IDqTaskExecution> Execution;
        ui64 ReservedCpuUs = 0;
    };
    struct TWorkerWakeup {
        NActors::TActorId Actor;
        bool KnownReady;
    };
    using TWakeup = std::function<void(TWorkerWakeup)>;

    TTaskWorkPool(ui32 maxQueryConcurrency, TWakeup wakeup,
        NMonitoring::TDynamicCounterPtr counters = {}, bool preferConsumers = false, bool preferReadyQueries = false);
    void RegisterGraph(NActors::TActorId query, ui64 registration,
        std::shared_ptr<const TQueryTaskGraph> graph, const TVector<ui64>& localTasks);
    void ForgetGraph(NActors::TActorId query, ui64 registration);
    void RegisterTask(const std::shared_ptr<TTaskSchedulingHandle>& task);
    void BindExecution(NActors::TActorId query, NActors::TActorId actor, std::shared_ptr<IDqTaskExecution> execution);
    void Report(ui64 registration, TTaskSchedulingSnapshot state);
    void Enqueue(std::shared_ptr<TTaskSchedulingHandle> task);
    void NotifyAvailable();
    // Returns no work and parks the worker atomically with respect to enqueue
    // and availability notifications. A zero worker ID performs a non-parking
    // inspection/claim, useful in deterministic tests.
    std::optional<TClaim> Take(NActors::TActorId worker, ui64 reserveCpuUs, const TLocalConsumerHint& hint = {});
    void Reschedule(NActors::TActorId worker);
    void Return(TClaim claim);
    void Finish(const TClaim& claim, ui64 cpuUs);
    void RecordActivation(bool ranSlice);
    void RecordExecution(const TTaskRunProgress& progress, ui64 cpuUs);
    void DeferQuery(NActors::TActorId query, TMonotonic until);
    void ResumeQuery(NActors::TActorId query, TMonotonic now = TMonotonic::Now());
    void SetQueryWeight(NActors::TActorId query, double weight);
    TQueryGraphSnapshot Describe(NActors::TActorId query, ui32 nodeId) const;

private:
    struct TTask {
        std::weak_ptr<TTaskSchedulingHandle> Handle;
        std::weak_ptr<IDqTaskExecution> Execution;
        std::shared_ptr<const TTaskExecutionLease> Available;
        std::shared_ptr<TTaskSchedulingHandle> Ready;
        TTaskSchedulingSnapshot State;
        ui64 Sequence = 0;
        ui64 SelectionAt = 0;
    };
    struct TQuery {
        ui64 Registration = 0;
        std::shared_ptr<const TQueryTaskGraph> Graph;
        THashSet<ui64> LocalTasks;
        THashMap<NActors::TActorId, TTask> Tasks;
        THashSet<NActors::TActorId> ReadyTasks;
        ui32 Running = 0;
        double Weight = 1;
        double Service = 0;
        ui64 ReservedCpuUs = 0;
        TMonotonic NotBefore;
    };
    ui32 CountWorkLocked(ui32 limit, ui32& knownReady) const;
    TVector<TWorkerWakeup> WakeLocked(NActors::TActorId preferred = {});
    void Wake(TVector<TWorkerWakeup> workers);

    const ui32 MaxQueryConcurrency;
    const TWakeup Wakeup;
    const bool PreferConsumers;
    const bool PreferReadyQueries;
    mutable std::mutex Mutex;
    THashMap<NActors::TActorId, TQuery> Queries;
    THashSet<NActors::TActorId> ReadyQueries;
    THashSet<NActors::TActorId> IdleWorkers;
    THashSet<NActors::TActorId> PendingWorkers;
    ui64 Sequence = 0;
    ui64 Selections = 0;
    double ServiceFloor = 0;
    NMonitoring::TDynamicCounters::TCounterPtr WorkerSlices, WorkerRuns, WorkerCpuUs, QuotaRejected, LeaseRetries, InlineCompletions, Wakeups, KnownReadyWakeups, EmptyTakes;
    NMonitoring::TDynamicCounters::TCounterPtr WorkerActivations, EmptyActivations;
public:
    void RecordInlineCompletion() { if (InlineCompletions) { ++*InlineCompletions; } }
private:
    NMonitoring::TDynamicCounters::TCounterPtr SelectionsCount, SelectionCpuUs, SelectionLockWaitUs;
    NMonitoring::TDynamicCounters::TCounterPtr RunnableSelections, RecheckSelections, BufferedOutputSelections;
    NMonitoring::TDynamicCounters::TCounterPtr LocalConsumerHints, LocalConsumerSelections, LocalConsumerPrioritySwitches;
    NMonitoring::TDynamicCounters::TCounterPtr ConsumerPrioritySwitches, ReadinessPriorityOverrides;
};

void StartDqComputeWorkers(NActors::TActorSystem* actorSystem, ui32 poolId,
    const std::shared_ptr<TTaskWorkPool>& pool, const TTaskSchedulerSettings& settings);
void WakeDqComputeWorker(NActors::TActorSystem* actorSystem, TTaskWorkPool::TWorkerWakeup wakeup);

struct TEvTaskSliceFinished : NActors::TEventLocal<TEvTaskSliceFinished, TDqComputeEvents::EvTaskSliceFinished> {
    TEvTaskSliceFinished(std::shared_ptr<IDqTaskExecution> execution, TTaskSliceResult result)
        : Execution(std::move(execution)), Result(std::move(result)) {}
    std::shared_ptr<IDqTaskExecution> Execution;
    TTaskSliceResult Result;
};

struct TEvTaskExecutionAvailable : NActors::TEventLocal<TEvTaskExecutionAvailable, TDqComputeEvents::EvTaskExecutionAvailable> {};

} // namespace NYql::NDq
