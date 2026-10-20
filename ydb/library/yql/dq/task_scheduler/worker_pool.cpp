#include "worker_pool.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/system/datetime.h>

#include <cmath>
#include <limits>
#include <tuple>

namespace NYql::NDq {
namespace {
bool IsTerminal(ETaskSchedulingState state) {
    return state == ETaskSchedulingState::Cancelled || state == ETaskSchedulingState::Finished;
}

bool KnownReady(const TTaskSchedulingSnapshot& state) {
    if (state.StopReason == ETaskStopReason::None || state.StopReason == ETaskStopReason::Runnable
        || state.StopReason == ETaskStopReason::Budget) {
        return true;
    }
    return !state.Waits.empty() && AllOf(state.Waits, [](const auto& wait) {
        return wait.Precision == ETaskWaitPrecision::Exact;
    });
}
} // namespace

TTaskWorkPool::TTaskWorkPool(ui32 maxQueryConcurrency, TWakeup wakeup, NMonitoring::TDynamicCounterPtr counters)
    : MaxQueryConcurrency(maxQueryConcurrency), Wakeup(std::move(wakeup))
{
    Y_ENSURE(MaxQueryConcurrency);
    if (counters) {
        auto group = counters->GetSubgroup("component", "TaskSchedulerWorkers");
        WorkerSlices = group->GetCounter("WorkerSlices", true);
        WorkerRuns = group->GetCounter("WorkerRuns", true);
        WorkerCpuUs = group->GetCounter("WorkerCpuUs", true);
        QuotaRejected = group->GetCounter("QuotaRejected", true);
        LeaseRetries = group->GetCounter("LeaseRetries", true);
        SelectionsCount = group->GetCounter("Selections", true);
        SelectionCpuUs = group->GetCounter("SelectionCpuUs", true);
        SelectionLockWaitUs = group->GetCounter("SelectionLockWaitUs", true);
    }
}

void TTaskWorkPool::Wake(TVector<NActors::TActorId> workers) {
    for (auto worker : workers) {
        Wakeup(worker);
    }
}

bool TTaskWorkPool::HasWorkLocked() const {
    const auto now = TMonotonic::Now();
    for (auto id : ReadyQueries) {
        const auto& query = Queries.at(id);
        if (query.Running >= MaxQueryConcurrency || query.NotBefore > now) {
            continue;
        }
        for (auto actor : query.ReadyTasks) {
            const auto& task = query.Tasks.at(actor);
            if (task.Ready) {
                if (task.Available && task.Available->load(std::memory_order_acquire) && !task.Execution.expired()) {
                    return true;
                }
            }
        }
    }
    return false;
}

TVector<NActors::TActorId> TTaskWorkPool::WakeLocked() {
    TVector<NActors::TActorId> workers;
    if (HasWorkLocked()) {
        workers.assign(IdleWorkers.begin(), IdleWorkers.end());
        IdleWorkers.clear();
    }
    return workers;
}

void TTaskWorkPool::RegisterGraph(NActors::TActorId id, ui64 registration,
    std::shared_ptr<const TQueryTaskGraph> graph, const TVector<ui64>& localTasks)
{
    std::lock_guard lock(Mutex);
    auto [it, inserted] = Queries.try_emplace(id);
    auto& query = it->second;
    if (inserted) {
        query.Service = ServiceFloor;
    }
    if (query.Registration > registration) {
        return;
    }
    query.Registration = registration;
    query.Graph = std::move(graph);
    query.LocalTasks.insert(localTasks.begin(), localTasks.end());
}

void TTaskWorkPool::ForgetGraph(NActors::TActorId id, ui64 registration) {
    std::optional<TQuery> retired;
    std::lock_guard lock(Mutex);
    auto it = Queries.find(id);
    if (it != Queries.end() && it->second.Registration == registration) {
        retired.emplace(std::move(it->second));
        ReadyQueries.erase(id);
        Queries.erase(it);
    }
}

void TTaskWorkPool::RegisterTask(const std::shared_ptr<TTaskSchedulingHandle>& handle) {
    const auto snapshot = handle->GetSnapshot();
    std::lock_guard lock(Mutex);
    auto [it, inserted] = Queries.try_emplace(handle->Query);
    if (inserted) {
        it->second.Service = ServiceFloor;
    }
    auto& task = it->second.Tasks[handle->Actor];
    task.Handle = handle;
    task.State = snapshot;
}

void TTaskWorkPool::BindExecution(NActors::TActorId query, NActors::TActorId actor,
                                 std::shared_ptr<IDqTaskExecution> execution) {
    TVector<NActors::TActorId> wake;
    const auto available = execution->GetAvailability();
    {
        std::lock_guard lock(Mutex);
        Queries.at(query).Tasks.at(actor).Execution = std::move(execution);
        Queries.at(query).Tasks.at(actor).Available = available;
        wake = WakeLocked();
    }
    Wake(std::move(wake));
}

void TTaskWorkPool::Report(ui64 registration, TTaskSchedulingSnapshot state) {
    std::shared_ptr<TTaskSchedulingHandle> retired;
    std::optional<TQuery> retiredQuery;
    {
        std::lock_guard lock(Mutex);
        auto query = Queries.find(state.Query);
        if (query == Queries.end() || query->second.Registration != registration) {
            return;
        }
        auto task = query->second.Tasks.find(state.Actor);
        if (task == query->second.Tasks.end() || task->second.State.Revision > state.Revision) {
            return;
        }
        task->second.State = std::move(state);
        if (IsTerminal(task->second.State.State)) {
            retired = std::move(task->second.Ready);
            query->second.ReadyTasks.erase(task->first);
            if (query->second.ReadyTasks.empty()) {
                ReadyQueries.erase(query->first);
            }
            if (!query->second.Graph && AllOf(query->second.Tasks, [](const auto& entry) {
                return IsTerminal(entry.second.State.State);
            })) {
                retiredQuery.emplace(std::move(query->second));
                Queries.erase(query);
            }
        }
    }
    // A handle can release the last graph registration. Never destroy it under
    // the pool lock: registration cleanup takes that same lock.
}

void TTaskWorkPool::Enqueue(std::shared_ptr<TTaskSchedulingHandle> handle) {
    const auto state = handle->GetSnapshot();
    TVector<NActors::TActorId> wake;
    {
        std::lock_guard lock(Mutex);
        auto query = Queries.find(handle->Query);
        if (query == Queries.end()) {
            return;
        }
        auto task = query->second.Tasks.find(handle->Actor);
        if (task == query->second.Tasks.end() || IsTerminal(task->second.State.State) || IsTerminal(state.State)) {
            return;
        }
        if (!task->second.Ready) {
            task->second.Sequence = ++Sequence;
            task->second.SelectionAt = Selections;
        }
        if (query->second.ReadyTasks.empty() && !query->second.Running) {
            query->second.Service = Max(query->second.Service, ServiceFloor);
        }
        query->second.ReadyTasks.insert(handle->Actor);
        ReadyQueries.insert(handle->Query);
        task->second.Ready = handle;
        if (state.Revision >= task->second.State.Revision) {
            task->second.State = state;
        }
        wake = WakeLocked();
    }
    Wake(std::move(wake));
}

void TTaskWorkPool::NotifyAvailable() {
    TVector<NActors::TActorId> wake;
    {
        std::lock_guard lock(Mutex);
        wake = WakeLocked();
    }
    Wake(std::move(wake));
}

std::optional<TTaskWorkPool::TClaim> TTaskWorkPool::Take(NActors::TActorId worker, ui64 reserveCpuUs) {
    const auto cpuStart = SelectionCpuUs ? ThreadCPUTime() : 0;
    const auto lockStart = SelectionLockWaitUs ? TMonotonic::Now() : TMonotonic();
    Y_DEFER { if (SelectionCpuUs) { *SelectionCpuUs += ThreadCPUTime() - cpuStart; } };
    std::unique_lock lock(Mutex);
    if (SelectionLockWaitUs) {
        *SelectionLockWaitUs += (TMonotonic::Now() - lockStart).MicroSeconds();
    }
    TQuery* selectedQuery = nullptr;
    TTask* selectedTask = nullptr;
    double service = std::numeric_limits<double>::infinity();
    std::tuple<bool, bool, ui32, ui64> taskKey;
    NActors::TActorId selectedQueryId, selectedActor;
    const auto now = TMonotonic::Now();
    for (auto id : ReadyQueries) {
        auto& query = Queries.at(id);
        if (query.Running >= MaxQueryConcurrency || query.NotBefore > now) {
            continue;
        }
        const double candidateService = query.Service + query.ReservedCpuUs / query.Weight;
        for (auto actor : query.ReadyTasks) {
            auto& task = query.Tasks.at(actor);
            if (!task.Ready) {
                continue;
            }
            if (!task.Available || !task.Available->load(std::memory_order_acquire) || task.Execution.expired()) {
                continue;
            }
            const auto distance = query.Graph ? query.Graph->GetResultDistance(task.State.TaskId) : std::nullopt;
            const auto key = std::tuple{Selections - task.SelectionAt < 64, !KnownReady(task.State),
                distance.value_or(std::numeric_limits<ui32>::max()), task.Sequence};
            if (!selectedTask || candidateService < service
                || (candidateService == service && (selectedQuery == &query ? key < taskKey
                    : task.Sequence < selectedTask->Sequence))) {
                selectedQuery = &query;
                selectedQueryId = id;
                selectedActor = actor;
                selectedTask = &task;
                service = candidateService;
                taskKey = key;
            }
        }
    }
    if (!selectedTask) {
        if (worker) {
            IdleWorkers.insert(worker);
        }
        return {};
    }
    ++Selections;
    if (SelectionsCount) {
        ++*SelectionsCount;
    }
    ++selectedQuery->Running;
    selectedQuery->ReservedCpuUs += reserveCpuUs;
    ServiceFloor = Max(ServiceFloor, service);
    auto task = std::move(selectedTask->Ready);
    auto execution = selectedTask->Execution;
    selectedQuery->ReadyTasks.erase(selectedActor);
    if (selectedQuery->ReadyTasks.empty()) {
        ReadyQueries.erase(selectedQueryId);
    }
    lock.unlock();
    return TClaim{std::move(task), execution.lock(), reserveCpuUs};
}

void TTaskWorkPool::Finish(const TClaim& claim, ui64 cpuUs) {
    TVector<NActors::TActorId> wake;
    {
        std::lock_guard lock(Mutex);
        auto query = Queries.find(claim.Task->Query);
        if (query != Queries.end()) {
            Y_ABORT_UNLESS(query->second.Running && query->second.ReservedCpuUs >= claim.ReservedCpuUs);
            --query->second.Running;
            query->second.ReservedCpuUs -= claim.ReservedCpuUs;
            query->second.Service += cpuUs / query->second.Weight;
        }
        wake = WakeLocked();
    }
    Wake(std::move(wake));
}

void TTaskWorkPool::Return(TClaim claim) {
    if (LeaseRetries) {
        ++*LeaseRetries;
    }
    Finish(claim, 0);
    Enqueue(std::move(claim.Task));
}

void TTaskWorkPool::RecordExecution(const TTaskRunProgress& progress, ui64 cpuUs) {
    if (WorkerSlices) {
        ++*WorkerSlices;
        *WorkerRuns += progress.RunnerInvoked;
        *WorkerCpuUs += cpuUs;
        *QuotaRejected += progress.QuotaRejected;
    }
}

void TTaskWorkPool::DeferQuery(NActors::TActorId id, TMonotonic until) {
    std::lock_guard lock(Mutex);
    if (auto query = Queries.find(id); query != Queries.end()) {
        query->second.NotBefore = Max(query->second.NotBefore, until);
    }
}

void TTaskWorkPool::ResumeQuery(NActors::TActorId id, TMonotonic now) {
    TVector<NActors::TActorId> wake;
    {
        std::lock_guard lock(Mutex);
        if (auto query = Queries.find(id); query != Queries.end() && query->second.NotBefore <= now) {
            query->second.NotBefore = {};
            wake = WakeLocked();
        }
    }
    Wake(std::move(wake));
}

void TTaskWorkPool::SetQueryWeight(NActors::TActorId query, double weight) {
    Y_ENSURE(std::isfinite(weight) && weight > 0);
    std::lock_guard lock(Mutex);
    Queries.at(query).Weight = weight;
}

TQueryGraphSnapshot TTaskWorkPool::Describe(NActors::TActorId id, ui32 nodeId) const {
    TQueryGraphSnapshot result;
    result.Query = id;
    result.NodeId = nodeId;
    {
        std::lock_guard lock(Mutex);
        auto query = Queries.find(id);
        if (query == Queries.end()) {
            return result;
        }
        result.Graph = query->second.Graph;
        result.LocalTaskIds.assign(query->second.LocalTasks.begin(), query->second.LocalTasks.end());
        for (const auto& [actor, task] : query->second.Tasks) {
            result.ManagedTasks.push_back(task.State);
        }
    }
    Sort(result.LocalTaskIds);
    Sort(result.ManagedTasks, [](const auto& left, const auto& right) { return left.TaskId < right.TaskId; });
    return result;
}

namespace {
struct TEvWorkerWakeup : NActors::TEventLocal<TEvWorkerWakeup, TDqComputeEvents::EvTaskWorkerWakeup> {};
struct TEvWorkerRetry : NActors::TEventLocal<TEvWorkerRetry, TDqComputeEvents::EvTaskWorkerRetry> {
    TEvWorkerRetry(std::weak_ptr<TTaskSchedulingHandle> task, NActors::TActorId query) : Task(std::move(task)), Query(query) {}
    std::weak_ptr<TTaskSchedulingHandle> Task;
    NActors::TActorId Query;
};

class TComputeWorker : public NActors::TActorBootstrapped<TComputeWorker> {
public:
    TComputeWorker(std::shared_ptr<TTaskWorkPool> pool, TTaskSchedulerSettings settings)
        : Pool(std::move(pool)), Settings(settings) {}
    void Bootstrap() {
        Become(&TComputeWorker::StateWork);
        Execute();
    }
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvWorkerWakeup::EventType, Execute);
            hFunc(TEvWorkerRetry, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }
private:
    void Handle(TEvWorkerRetry::TPtr& ev) {
        Pool->ResumeQuery(ev->Get()->Query);
        if (auto task = ev->Get()->Task.lock()) {
            task->RequestExecution();
        }
    }
    void Execute() {
        const auto deadline = TMonotonic::Now() + TDuration::MicroSeconds(Settings.WorkerActivationUs);
        do {
            auto claim = Pool->Take(SelfId(), Settings.WorkerSliceCpuUs);
            if (!claim) {
                return;
            }
            if (!claim->Execution) {
                claim->Task->Cancel();
                Pool->Finish(*claim, 0);
                continue;
            }
            if (!claim->Execution->TryAcquire()) {
                Pool->Return(std::move(*claim));
                continue;
            }
            TTaskSliceResult result;
            const auto cpuStart = ThreadCPUTime();
            {
                Y_DEFER { claim->Execution->Release(); };
                auto generation = claim->Task->TryDispatch();
                if (!generation || !claim->Task->Start(*generation)) {
                    Pool->Finish(*claim, 0);
                    continue;
                }
                try {
                    result = claim->Execution->RunSlice({Settings.WorkerSliceCpuUs, deadline});
                } catch (...) {
                    result.Error = std::current_exception();
                }
            }
            const auto cpuUs = Max<ui64>(1, ThreadCPUTime() - cpuStart);
            Pool->RecordExecution(result.Progress, cpuUs);
            if (result.RetryAfter) {
                Pool->DeferQuery(claim->Task->Query, TMonotonic::Now() + *result.RetryAfter);
            }
            Pool->Finish(*claim, cpuUs);
            if (result.RetryAfter) {
                Schedule(*result.RetryAfter, new TEvWorkerRetry(claim->Task, claim->Task->Query));
            }
            claim->Execution->OnSliceFinished(std::move(result));
        } while (TMonotonic::Now() < deadline);
        Send(SelfId(), new TEvWorkerWakeup());
    }
    const std::shared_ptr<TTaskWorkPool> Pool;
    const TTaskSchedulerSettings Settings;
};
} // namespace

void StartDqComputeWorkers(NActors::TActorSystem* actorSystem, ui32 poolId,
    const std::shared_ptr<TTaskWorkPool>& pool, const TTaskSchedulerSettings& settings)
{
    for (ui32 i = 0; i < settings.Shards * settings.MaxInFlightPerShard; ++i) {
        actorSystem->Register(new TComputeWorker(pool, settings), NActors::TMailboxType::HTSwap, poolId);
    }
}

// Kept here so callers do not depend on the worker's private event type.
void WakeDqComputeWorker(NActors::TActorSystem* actorSystem, NActors::TActorId worker) {
    actorSystem->Send(worker, new TEvWorkerWakeup());
}

} // namespace NYql::NDq
