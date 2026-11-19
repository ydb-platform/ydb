#include "worker_pool.h"
#include "worker_continuation.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/system/datetime.h>
#include <util/system/env.h>
#include <util/string/cast.h>

#include <cmath>
#include <limits>
#include <tuple>
#include <utility>

namespace NYql::NDq {
namespace {
thread_local TLocalConsumerHint* ActiveLocalConsumerHint = nullptr;
thread_local const TTaskWorkPool* ContinuingWorkPool = nullptr;
thread_local bool InsideWorkerActivation = false;
constexpr ui64 AgingSelections = 64;
constexpr double RecheckPenaltyUs = 50;

bool IsTerminal(ETaskSchedulingState state) {
    return state == ETaskSchedulingState::Cancelled || state == ETaskSchedulingState::Finished;
}

bool KnownReady(const TTaskSchedulingSnapshot& state) {
    return state.Classification.Readiness == ETaskReadiness::Runnable;
}

ui32 WorkRank(ETaskWorkKind work) {
    switch (work) {
        case ETaskWorkKind::Emit: return 0;
        case ETaskWorkKind::Control: return 1;
        case ETaskWorkKind::Consume: return 2;
        case ETaskWorkKind::Compute: return 3;
        case ETaskWorkKind::Unknown: return 4;
    }
    return 4;
}
} // namespace

TWorkerContinuationScope::TWorkerContinuationScope(const TTaskWorkPool& pool) noexcept
    : Previous(std::exchange(ContinuingWorkPool, &pool)) {}

TWorkerContinuationScope::~TWorkerContinuationScope() {
    ContinuingWorkPool = Previous;
}

bool TWorkerContinuationScope::IsActive(const TTaskWorkPool& pool) noexcept {
    return ContinuingWorkPool == &pool;
}

TLocalConsumerScope::TLocalConsumerScope(TLocalConsumerHint& hint, NActors::TActorId query, ui64 producerTask)
    : Previous(std::exchange(ActiveLocalConsumerHint, &hint)) {
    hint = {.Query = query, .ProducerTask = producerTask};
}

TLocalConsumerScope::~TLocalConsumerScope() {
    ActiveLocalConsumerHint = Previous;
}

bool TLocalConsumerScope::Captures(NActors::TActorId query) {
    return ActiveLocalConsumerHint && ActiveLocalConsumerHint->Query == query;
}

void TLocalConsumerScope::Record(NActors::TActorId actor, ui64 readyRevision, ui64 channel) {
    ActiveLocalConsumerHint->Consumer = actor;
    ActiveLocalConsumerHint->ReadyRevision = readyRevision;
    ActiveLocalConsumerHint->Channel = channel;
}

TTaskWorkPool::TTaskWorkPool(ui32 maxQueryConcurrency, TWakeup wakeup,
    NMonitoring::TDynamicCounterPtr counters, bool preferConsumers, bool preferReadyQueries)
    : MaxQueryConcurrency(maxQueryConcurrency), Wakeup(std::move(wakeup))
    , PreferConsumers(preferConsumers), PreferReadyQueries(preferReadyQueries)
{
    Y_ENSURE(MaxQueryConcurrency);
    if (counters) {
        auto group = counters->GetSubgroup("component", "TaskSchedulerWorkers");
        WorkerSlices = group->GetCounter("WorkerSlices", true);
        WorkerRuns = group->GetCounter("WorkerRuns", true);
        WorkerCpuUs = group->GetCounter("WorkerCpuUs", true);
        QuotaRejected = group->GetCounter("QuotaRejected", true);
        LeaseRetries = group->GetCounter("LeaseRetries", true);
        InlineCompletions = group->GetCounter("InlineCompletions", true);
        Wakeups = group->GetCounter("Wakeups", true);
        KnownReadyWakeups = group->GetCounter("KnownReadyWakeups", true);
        EmptyTakes = group->GetCounter("EmptyTakes", true);
        WorkerActivations = group->GetCounter("WorkerActivations", true);
        EmptyActivations = group->GetCounter("EmptyActivations", true);
        LocalConsumerHints = group->GetCounter("LocalConsumerHints", true);
        LocalConsumerSelections = group->GetCounter("LocalConsumerSelections", true);
        LocalConsumerPrioritySwitches = group->GetCounter("LocalConsumerPrioritySwitches", true);
        ConsumerPrioritySwitches = group->GetCounter("ConsumerPrioritySwitches", true);
        ReadinessPriorityOverrides = group->GetCounter("ReadinessPriorityOverrides", true);
        SelectionsCount = group->GetCounter("Selections", true);
        SelectionCpuUs = group->GetCounter("SelectionCpuUs", true);
        SelectionLockWaitUs = group->GetCounter("SelectionLockWaitUs", true);
        RunnableSelections = group->GetCounter("RunnableSelections", true);
        RecheckSelections = group->GetCounter("RecheckSelections", true);
        BufferedOutputSelections = group->GetCounter("BufferedOutputSelections", true);
    }
}

void TTaskWorkPool::Wake(TVector<TWorkerWakeup> workers) {
    for (auto worker : workers) {
        Wakeup(worker);
    }
}

ui32 TTaskWorkPool::CountWorkLocked(ui32 limit, ui32& knownReady) const {
    // Claimable tasks, capped by `limit` and by each query's free concurrency.
    ui32 count = 0;
    const auto now = TMonotonic::Now();
    for (auto id : ReadyQueries) {
        const auto& query = Queries.at(id);
        if (query.Running >= MaxQueryConcurrency || query.NotBefore > now) {
            continue;
        }
        ui32 queryCount = 0;
        const ui32 queryLimit = MaxQueryConcurrency - query.Running;
        for (auto actor : query.ReadyTasks) {
            const auto& task = query.Tasks.at(actor);
            if (task.Ready && task.Available && !task.Execution.expired() && task.Available->IsAvailableOrSubscribe()) {
                if (KnownReady(task.State)) {
                    ++knownReady;
                }
                if (++queryCount >= queryLimit) {
                    break;
                }
            }
        }
        count += queryCount;
        if (count >= limit) {
            return limit;
        }
    }
    return count;
}

TVector<TTaskWorkPool::TWorkerWakeup> TTaskWorkPool::WakeLocked(NActors::TActorId preferred) {
    TVector<TWorkerWakeup> workers;
    if (IdleWorkers.empty()) {
        return workers;
    }
    // Each notification already on the way covers one claimable task until the
    // recipient enters Take. Availability changes can arrive before delivery.
    const ui32 pending = PendingWorkers.size();
    // Release, Finish and inline completion can all make work available just
    // before this worker calls Take again. Leave one claim for that selection.
    // Other threads and producers still wake workers normally; excess ready
    // tasks can use idle workers even during this completion.
    const ui32 covered = pending + TWorkerContinuationScope::IsActive(*this);
    ui32 knownReady = 0;
    const ui32 claimable = CountWorkLocked(IdleWorkers.size() + covered, knownReady);
    if (claimable <= covered) {
        return workers;
    }
    const ui32 count = claimable - covered;
    const auto wake = [&](auto it) {
        // Outstanding workers may claim any task. Only expedite readiness
        // beyond the work already covered; Take still decides what to run.
        workers.push_back({*it, covered + workers.size() < knownReady});
        PendingWorkers.insert(*it);
        IdleWorkers.erase(it);
    };
    if (auto it = IdleWorkers.find(preferred); it != IdleWorkers.end()) {
        wake(it);
    }
    while (workers.size() < count) {
        wake(IdleWorkers.begin());
    }
    if (Wakeups) {
        *Wakeups += workers.size();
        if (knownReady > covered) {
            *KnownReadyWakeups += Min<ui32>(count, knownReady - covered);
        }
    }
    return workers;
}

void TTaskWorkPool::Reschedule(NActors::TActorId worker) {
    TVector<TWorkerWakeup> wake;
    {
        std::lock_guard lock(Mutex);
        // Publish that this worker is idle under the same lock as Enqueue, so
        // arriving work either participates in this selection or wakes it later.
        IdleWorkers.insert(worker);
        wake = WakeLocked(worker);
    }
    Wake(std::move(wake));
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
    TVector<TWorkerWakeup> wake;
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
    TVector<TWorkerWakeup> wake;
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
    TVector<TWorkerWakeup> wake;
    {
        std::lock_guard lock(Mutex);
        wake = WakeLocked();
    }
    Wake(std::move(wake));
}

std::optional<TTaskWorkPool::TClaim> TTaskWorkPool::Take(NActors::TActorId worker, ui64 reserveCpuUs,
    const TLocalConsumerHint& hint)
{
    const auto cpuStart = SelectionCpuUs ? ThreadCPUTime() : 0;
    const auto lockStart = SelectionLockWaitUs ? TMonotonic::Now() : TMonotonic();
    Y_DEFER { if (SelectionCpuUs) { *SelectionCpuUs += ThreadCPUTime() - cpuStart; } };
    std::unique_lock lock(Mutex);
    if (SelectionLockWaitUs) {
        *SelectionLockWaitUs += (TMonotonic::Now() - lockStart).MicroSeconds();
    }
    if (worker && !PendingWorkers.empty()) {
        PendingWorkers.erase(worker);
    }
    TQuery* selectedQuery = nullptr;
    TTask* selectedTask = nullptr;
    double service = std::numeric_limits<double>::infinity();
    double score = service;
    double leastService = service;
    std::tuple<bool, bool, ui32, ui32, ui32, ui64> taskKey;
    NActors::TActorId selectedQueryId, selectedActor;
    const auto now = TMonotonic::Now();
    const auto eligible = [](const TTask& task) {
        return task.Ready && task.Available && !task.Execution.expired() && task.Available->IsAvailableOrSubscribe();
    };
    const auto keyFor = [this](const TQuery& query, const TTask& task) {
        const auto distance = query.Graph ? query.Graph->GetResultDistance(task.State.TaskId) : std::nullopt;
        return std::tuple{Selections - task.SelectionAt < AgingSelections, !KnownReady(task.State),
            PreferConsumers ? static_cast<ui32>(task.State.Priority) : 0u,
            distance.value_or(std::numeric_limits<ui32>::max()), WorkRank(task.State.Classification.Work), task.Sequence};
    };
    for (auto id : ReadyQueries) {
        auto& query = Queries.at(id);
        if (query.Running >= MaxQueryConcurrency || query.NotBefore > now) {
            continue;
        }
        const double candidateService = query.Service + query.ReservedCpuUs / query.Weight;
        if (candidateService > score) {
            continue;
        }
        for (auto actor : query.ReadyTasks) {
            auto& task = query.Tasks.at(actor);
            if (!eligible(task)) {
                continue;
            }
            const auto key = keyFor(query, task);
            if (PreferReadyQueries) {
                leastService = Min(leastService, candidateService);
            }
            // An uncertain task can lose to ready work in a nearby CPU-service
            // band. Remove the penalty after it has waited through enough picks.
            const double candidateScore = candidateService
                + (PreferReadyQueries && std::get<0>(key) && std::get<1>(key) ? RecheckPenaltyUs : 0.0);
            if (!selectedTask || candidateScore < score
                || (candidateScore == score && (selectedQuery == &query ? key < taskKey
                    : task.Sequence < selectedTask->Sequence))) {
                selectedQuery = &query;
                selectedQueryId = id;
                selectedActor = actor;
                selectedTask = &task;
                service = candidateService;
                score = candidateScore;
                taskKey = key;
            }
        }
    }
    bool consumerPriorityChangedSelection = false;
    if (selectedTask && PreferConsumers && selectedQuery->ReadyTasks.size() > 1) {
        const TTask* baseline = nullptr;
        std::tuple<bool, bool, ui32, ui32, ui64> baselineKey;
        for (auto actor : selectedQuery->ReadyTasks) {
            const auto& candidate = selectedQuery->Tasks.at(actor);
            if (!eligible(candidate)) {
                continue;
            }
            const auto key = keyFor(*selectedQuery, candidate);
            const auto withoutPriority = std::tuple{std::get<0>(key), std::get<1>(key),
                std::get<3>(key), std::get<4>(key), std::get<5>(key)};
            if (!baseline || withoutPriority < baselineKey) {
                baseline = &candidate;
                baselineKey = withoutPriority;
            }
        }
        consumerPriorityChangedSelection = baseline != selectedTask;
        if (ConsumerPrioritySwitches) {
            *ConsumerPrioritySwitches += consumerPriorityChangedSelection;
        }
    }
    bool localConsumer = false;
    bool localityChangedSelection = false;
    if (hint.Consumer && LocalConsumerHints) {
        ++*LocalConsumerHints;
    }
    // Let the normal policy select the query first, including its cross-query
    // tie breaker. Within it, locality never beats aging or known readiness.
    if (selectedTask && hint.Consumer && hint.Query == selectedQueryId) {
        auto it = selectedQuery->Tasks.find(hint.Consumer);
        if (it != selectedQuery->Tasks.end() && eligible(it->second) && it->second.State.Revision == hint.ReadyRevision) {
            const auto consumerKey = keyFor(*selectedQuery, it->second);
            if (std::tie(std::get<0>(consumerKey), std::get<1>(consumerKey), std::get<2>(consumerKey))
                <= std::tie(std::get<0>(taskKey), std::get<1>(taskKey), std::get<2>(taskKey)))
            {
                localConsumer = true;
                localityChangedSelection = selectedActor != hint.Consumer;
                selectedActor = hint.Consumer;
                selectedTask = &it->second;
                taskKey = consumerKey;
                if (LocalConsumerSelections) {
                    ++*LocalConsumerSelections;
                    *LocalConsumerPrioritySwitches += localityChangedSelection;
                }
            }
        }
    }
    if (!selectedTask) {
        if (EmptyTakes) {
            ++*EmptyTakes;
        }
        if (worker) {
            IdleWorkers.insert(worker);
        }
        return {};
    }
    ++Selections;
    if (PreferReadyQueries && ReadinessPriorityOverrides && service > leastService) {
        ++*ReadinessPriorityOverrides;
    }
    if (SelectionsCount) {
        ++*SelectionsCount;
        ++*(KnownReady(selectedTask->State) ? RunnableSelections : RecheckSelections);
        if (selectedTask->State.Classification.Work == ETaskWorkKind::Emit) { ++*BufferedOutputSelections; }
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
    NJson::TJsonValue selection;
    if (task->Trace) {
        selection.SetType(NJson::JSON_MAP);
        selection["worker"] = ToString(worker);
        selection["query_service_score"] = service;
        if (PreferReadyQueries) {
            selection["query_priority_score"] = score;
        }
        selection["query_weight"] = selectedQuery->Weight;
        selection["query_running"] = selectedQuery->Running;
        selection["reserved_cpu_us"] = reserveCpuUs;
        selection["aging_override"] = !std::get<0>(taskKey);
        selection["known_ready"] = !std::get<1>(taskKey);
        selection["readiness"] = TaskReadinessName(selectedTask->State.Classification.Readiness);
        selection["work"] = TaskWorkName(selectedTask->State.Classification.Work);
        selection["coverage"] = TaskCoverageName(selectedTask->State.Classification.Coverage);
        selection["work_rank"] = std::get<4>(taskKey);
        selection["consumer_rank"] = std::get<2>(taskKey);
        selection["consumer_priority_changed_selection"] = consumerPriorityChangedSelection;
        selection["result_distance"] = std::get<3>(taskKey) == std::numeric_limits<ui32>::max()
            ? NJson::TJsonValue() : NJson::TJsonValue(std::get<3>(taskKey));
        selection["fifo_sequence"] = std::get<5>(taskKey);
        if (localConsumer) {
            selection["local_consumer"] = true;
            selection["locality_changed_selection"] = localityChangedSelection;
            selection["local_producer_task"] = hint.ProducerTask;
            selection["local_channel"] = hint.Channel;
        }
        selection["selection"] = Selections;
    }
    const auto selectedAt = task->Trace ? TMonotonic::Now() : TMonotonic();
    lock.unlock();
    if (task->Trace) { task->Trace->Event("selected", task->TaskId, task->Actor, std::move(selection), selectedAt); }
    return TClaim{std::move(task), execution.lock(), reserveCpuUs};
}

void TTaskWorkPool::Finish(const TClaim& claim, ui64 cpuUs) {
    TVector<TWorkerWakeup> wake;
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
    if (claim.Task->Trace) {
        claim.Task->Trace->Event("lease_retry", claim.Task->TaskId, claim.Task->Actor, NJson::TJsonValue(NJson::JSON_MAP));
    }
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

void TTaskWorkPool::RecordActivation(bool ranSlice) {
    if (WorkerActivations) {
        ++*WorkerActivations;
        if (!ranSlice) {
            ++*EmptyActivations;
        }
    }
}

void TTaskWorkPool::DeferQuery(NActors::TActorId id, TMonotonic until) {
    std::lock_guard lock(Mutex);
    if (auto query = Queries.find(id); query != Queries.end()) {
        query->second.NotBefore = Max(query->second.NotBefore, until);
    }
}

void TTaskWorkPool::ResumeQuery(NActors::TActorId id, TMonotonic now) {
    TVector<TWorkerWakeup> wake;
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
        const bool previous = std::exchange(InsideWorkerActivation, true);
        Y_DEFER { InsideWorkerActivation = previous; };
        const auto deadline = TMonotonic::Now() + TDuration::MicroSeconds(Settings.WorkerActivationUs);
        TLocalConsumerHint hint;
        bool ranSlice = false;
        Y_DEFER { Pool->RecordActivation(ranSlice); };
        do {
            // A hint expires at the next selection or at the end of this
            // activation, whichever comes first.
            auto claim = Pool->Take(SelfId(), Settings.WorkerSliceCpuUs, hint);
            hint = {};
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
            std::optional<TWorkerContinuationScope> continuation;
            const auto cpuStart = ThreadCPUTime();
            const auto sliceStart = claim->Task->Trace ? TMonotonic::Now() : TMonotonic();
            {
                Y_DEFER { claim->Execution->Release(); };
                auto generation = claim->Task->TryDispatch();
                if (!generation || !claim->Task->Start(*generation)) {
                    Pool->Finish(*claim, 0);
                    continue;
                }
                ranSlice = true;
                try {
                    std::optional<TLocalConsumerScope> locality;
                    if (Settings.PreferLocalConsumer) {
                        locality.emplace(hint, claim->Task->Query, claim->Task->TaskId);
                    }
                    result = claim->Execution->RunSlice({Settings.WorkerSliceCpuUs, deadline});
                } catch (...) {
                    result.Error = std::current_exception();
                }
                if (result.Error || result.RetryAfter || result.Progress.QuotaRejected) {
                    hint = {};
                }
                if (TMonotonic::Now() < deadline) {
                    continuation.emplace(*Pool);
                }
            }
            const auto cpuUs = Max<ui64>(1, ThreadCPUTime() - cpuStart);
            if (claim->Task->Trace) {
                NJson::TJsonValue args(NJson::JSON_MAP);
                args["worker"] = ToString(SelfId());
                args["cpu_us"] = cpuUs;
                args["requested_cpu_us"] = Settings.WorkerSliceCpuUs;
                args["runner_invoked"] = result.Progress.RunnerInvoked;
                args["quota_rejected"] = result.Progress.QuotaRejected;
                args["failed"] = bool(result.Error);
                args["runner_status"] = result.Status;
                if (result.RetryAfter) { args["retry_after_us"] = result.RetryAfter->MicroSeconds(); }
                claim->Task->Trace->Event("worker_slice", claim->Task->TaskId, claim->Task->Actor,
                    std::move(args), sliceStart, (TMonotonic::Now() - sliceStart).MicroSeconds());
            }
            Pool->RecordExecution(result.Progress, cpuUs);
            if (result.RetryAfter) {
                Pool->DeferQuery(claim->Task->Query, TMonotonic::Now() + *result.RetryAfter);
            }
            Pool->Finish(*claim, cpuUs);
            if (result.RetryAfter) {
                Schedule(*result.RetryAfter, new TEvWorkerRetry(claim->Task, claim->Task->Query));
            }
            if (result.CompleteInline) {
                Pool->RecordInlineCompletion();
            }
            claim->Execution->OnSliceFinished(std::move(result));
        } while (TMonotonic::Now() < deadline);
        Pool->Reschedule(SelfId());
    }
    const std::shared_ptr<TTaskWorkPool> Pool;
    const TTaskSchedulerSettings Settings;
};
} // namespace

void StartDqComputeWorkers(NActors::TActorSystem* actorSystem, ui32 poolId,
    const std::shared_ptr<TTaskWorkPool>& pool, const TTaskSchedulerSettings& settings)
{
    // Workers run in a dedicated executor pool named "DqWorkers" when the actor system has one,
    // so their activations do not queue behind compute-actor mailboxes; otherwise in `poolId`.
    for (auto* executorPool : actorSystem->GetBasicExecutorPools()) {
        if (executorPool && executorPool->GetName() == "DqWorkers") {
            poolId = executorPool->PoolId;
            break;
        }
    }
    for (ui32 i = 0; i < settings.Shards * settings.MaxInFlightPerShard; ++i) {
        actorSystem->Register(new TComputeWorker(pool, settings), NActors::TMailboxType::HTSwap, poolId);
    }
}

// Kept here so callers do not depend on the worker's private event type.
void WakeDqComputeWorker(NActors::TActorSystem* actorSystem, TTaskWorkPool::TWorkerWakeup wakeup) {
    static const auto policy = GetEnv("YDB_DQ_TASK_TAIL_WAKE");
    static const bool all = policy == "1";
    static const bool selective = policy == "ready";
    const bool tailWake = all || (selective && wakeup.KnownReady);
    // Only the initial handoff may cut ahead of other mailboxes. A worker
    // yielding at its deadline or waking a peer uses regular delivery.
    if (tailWake && !InsideWorkerActivation && NActors::TlsActivationContext
        && NActors::TActivationContext::ActorSystem() == actorSystem)
    {
        NActors::TActivationContext::Send<NActors::ESendingType::Tail>(
            new NActors::IEventHandle(wakeup.Actor, NActors::TActorId{}, new TEvWorkerWakeup()));
        return;
    }
    actorSystem->Send(wakeup.Actor, new TEvWorkerWakeup());
}

} // namespace NYql::NDq
