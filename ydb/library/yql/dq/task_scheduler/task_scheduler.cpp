#include "task_scheduler.h"
#include "worker_pool.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>

#include <array>
#include <deque>
#include <limits>

namespace NYql::NDq {

namespace {
constexpr TTaskDependencyId AnyDependency = std::numeric_limits<ui64>::max();
} // namespace

struct TTaskSchedulerMetrics {
    NMonitoring::TDynamicCounters::TCounterPtr Notifications, Coalesced, Enqueued, Dispatched;
    NMonitoring::TDynamicCounters::TCounterPtr Runs, NoObservedProgressRuns, NoObservedProgressCpuUs;
    NMonitoring::TDynamicCounters::TCounterPtr Attempts, QuotaRejected, InputItems;
    NMonitoring::TDynamicCounters::TCounterPtr InputRows, InputBytes, OutputRows, OutputBytes, CpuUs, BudgetYields;
    NMonitoring::THistogramPtr RowsPerRun, OutputRowsPerRun, CpuUsPerRun, ElapsedUsPerRun, ReadyWaitUs;

    explicit TTaskSchedulerMetrics(NMonitoring::TDynamicCounterPtr counters) {
        auto group = counters->GetSubgroup("component", "TaskSchedulerPoC");
#define COUNTER(field) field = group->GetCounter(#field, true)
        COUNTER(Notifications);
        COUNTER(Coalesced);
        COUNTER(Enqueued);
        COUNTER(Dispatched);
        COUNTER(Runs);
        COUNTER(NoObservedProgressRuns);
        COUNTER(NoObservedProgressCpuUs);
        COUNTER(Attempts);
        COUNTER(QuotaRejected);
        COUNTER(InputItems);
        COUNTER(InputRows);
        COUNTER(InputBytes);
        COUNTER(OutputRows);
        COUNTER(OutputBytes);
        COUNTER(CpuUs);
        COUNTER(BudgetYields);
#undef COUNTER
        RowsPerRun = group->GetHistogram("InputRowsPerRun", NMonitoring::ExponentialHistogram(20, 2, 1));
        OutputRowsPerRun = group->GetHistogram("OutputRowsPerRun", NMonitoring::ExponentialHistogram(20, 2, 1));
        CpuUsPerRun = group->GetHistogram("CpuUsPerRun", NMonitoring::ExponentialHistogram(20, 2, 1));
        ElapsedUsPerRun = group->GetHistogram("ElapsedUsPerRun", NMonitoring::ExponentialHistogram(24, 2, 1));
        ReadyWaitUs = group->GetHistogram("ReadyWaitUs", NMonitoring::ExponentialHistogram(24, 2, 1));
    }

    void Report(const TTaskRunProgress& p) {
        ++*Attempts;
        *QuotaRejected += p.QuotaRejected;
        if (!p.RunnerInvoked) {
            return;
        }
        ++*Runs;
        *InputItems += p.InputItems;
        *InputRows += p.InputRows;
        *InputBytes += p.InputBytes;
        *OutputRows += p.OutputRows;
        *OutputBytes += p.OutputBytes;
        *CpuUs += p.CpuTimeUs;
        *BudgetYields += p.BudgetExhausted;
        RowsPerRun->Collect(p.InputRows);
        OutputRowsPerRun->Collect(p.OutputRows);
        CpuUsPerRun->Collect(p.CpuTimeUs);
        ElapsedUsPerRun->Collect(p.ElapsedTimeUs);
        if (!p.HasObservedProgress()) {
            ++*NoObservedProgressRuns;
            *NoObservedProgressCpuUs += p.CpuTimeUs;
        }
    }
};

TTaskWaitCondition TTaskWaitCondition::Any(TVector<TTaskDependencyVersion> dependencies) {
    TTaskWaitCondition result;
    for (auto dependency : dependencies) {
        result.AnyOf.push_back({dependency});
    }
    return result;
}

TTaskWaitCondition TTaskWaitCondition::All(TVector<TTaskDependencyVersion> dependencies) {
    TTaskWaitCondition result;
    if (!dependencies.empty()) {
        result.AnyOf.push_back(std::move(dependencies));
    }
    return result;
}

bool TTaskWaitCondition::IsSatisfied(const THashMap<TTaskDependencyId, ui64>& versions) const {
    for (const auto& group : AnyOf) {
        bool ready = !group.empty();
        for (const auto& dependency : group) {
            auto it = versions.find(dependency.Id);
            ready &= (it == versions.end() ? 0 : it->second) != dependency.Version;
        }
        if (ready) {
            return true;
        }
    }
    return false;
}

TTaskSchedulingHandle::TTaskSchedulingHandle(NActors::TActorId actor, NActors::TActorId query, TEnqueue enqueue, TRelease release,
                                             std::shared_ptr<TTaskSchedulerMetrics> metrics, bool observeOnly, ui64 taskId,
                                             TStateReporter reportState, std::shared_ptr<const TQueryGraphRegistration> graphRegistration,
                                             std::shared_ptr<TQueryTaskTrace> trace)
    : Actor(actor), Query(query), TaskId(taskId), ObserveOnly(observeOnly), Trace(std::move(trace)), Enqueue(std::move(enqueue)), Release(std::move(release)),
      Metrics(std::move(metrics)), ReportState(std::move(reportState)), GraphRegistration(std::move(graphRegistration)) {}

bool TTaskSchedulingHandle::MakeReadyLocked() {
    if (State != EState::Blocked) {
        return false;
    }
    State = EState::Ready;
    ++Revision;
    ReadySince = TMonotonic::Now();
    if (Trace) { Trace->State(SnapshotLocked()); }
    if (Metrics) {
        ++*Metrics->Enqueued;
    }
    return true;
}

void TTaskSchedulingHandle::RequestExecution() {
    bool enqueue;
    {
        std::lock_guard lock(Mutex);
        if (State == EState::Cancelled || State == EState::Finished) {
            return;
        }
        ++Versions[0];
        ++Versions[AnyDependency];
        Continue = true;
        const auto notifiedAt = Trace ? TMonotonic::Now() : TMonotonic();
        enqueue = MakeReadyLocked();
        if (Trace) {
            NJson::TJsonValue args(NJson::JSON_MAP);
            args["ready"] = enqueue;
            args["version"] = Versions[0];
            Trace->Event("continuation", TaskId, Actor, std::move(args), notifiedAt);
        }
        if (Metrics && !enqueue) {
            ++*Metrics->Coalesced;
        }
    }
    if (enqueue) {
        Enqueue(shared_from_this());
    }
}

void TTaskSchedulingHandle::Signal(TTaskDependencyId dependency, ETaskPriority priority) {
    bool enqueue = false;
    {
        std::lock_guard lock(Mutex);
        if (State == EState::Cancelled || State == EState::Finished) {
            return;
        }
        ++Versions[dependency];
        ++Versions[AnyDependency];
        const auto notifiedAt = Trace ? TMonotonic::Now() : TMonotonic();
        if (priority == ETaskPriority::Drain && Priority != priority) {
            Priority = priority;
            ++Revision;
        }
        if (Metrics) {
            ++*Metrics->Notifications;
        }
        if (State == EState::Blocked && Wait.IsSatisfied(Versions)) {
            enqueue = MakeReadyLocked();
        }
        if (Metrics && !enqueue) {
            ++*Metrics->Coalesced;
        }
        if (Trace) {
            NJson::TJsonValue args(NJson::JSON_MAP);
            args["dependency"] = ToString(dependency);
            args["version"] = Versions[dependency];
            args["ready"] = enqueue;
            Trace->Event("signal", TaskId, Actor, std::move(args), notifiedAt);
        }
    }
    if (enqueue) {
        Enqueue(shared_from_this());
    }
}

ui64 TTaskSchedulingHandle::Observe(TTaskDependencyId dependency) {
    std::lock_guard lock(Mutex);
    return Versions[dependency];
}

void TTaskSchedulingHandle::BeginProbe() {
    std::lock_guard lock(Mutex);
    ObservedWaits.clear();
    ExplicitWaits.clear();
    ProbeCoverage = ETaskWaitCoverage::Unknown;
    ExplicitRunnable = false;
}

std::pair<size_t, size_t> TTaskSchedulingHandle::BeginInputWait() const {
    std::lock_guard lock(Mutex);
    return {ObservedWaits.size(), ExplicitWaits.size()};
}

void TTaskSchedulingHandle::ReportInputWait(std::pair<size_t, size_t> scope, ui64 operatorId, ETaskInputSide side, TStringBuf name) {
    std::lock_guard lock(Mutex);
    // A child can report a more precise cause, including a spill operation.
    if (State != EState::Running) {
        return;
    }
    if (ExplicitWaits.size() > scope.second) {
        for (size_t i = scope.second; i < ExplicitWaits.size(); ++i) {
            ExplicitWaits[i].InputPath.push_back({operatorId, side, TString(name)});
        }
        return;
    }
    TTaskWaitDescription description;
    description.Reason = ETaskWaitReason::Input;
    description.Origin = ETaskWaitOrigin::Operator;
    description.Precision = ETaskWaitPrecision::ObservedInputs;
    description.OperatorId = operatorId;
    description.OperatorName = name;
    description.InputSide = side;
    TVector<TTaskDependencyVersion> dependencies;
    for (size_t i = scope.first; i < ObservedWaits.size(); ++i) {
        dependencies.push_back(ObservedWaits[i]);
    }
    if (dependencies.empty()) {
        // The child may be waiting for anything. The enclosing operator was
        // reading an input, but that does not identify the child's stop reason.
        description.Reason = ETaskWaitReason::Unknown;
        description.Precision = ETaskWaitPrecision::Opaque;
        dependencies.push_back({AnyDependency, AnyVersion});
    } else {
        // An uninstrumented child can probe an input and then wait for I/O.
        // Preserve legacy callbacks until the child reports its precise wait.
        dependencies.push_back({0, CallbackVersion});
        dependencies.push_back({SpillingDependency, SpillingVersion});
    }
    description.Condition = TTaskWaitCondition::Any(std::move(dependencies));
    ExplicitWaits.push_back(std::move(description));
}

void TTaskSchedulingHandle::ReportWait(TTaskWaitDescription description) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        ExplicitWaits.push_back(std::move(description));
    }
}

void TTaskSchedulingHandle::ReportCallbackWait(ui64 operatorId, ETaskWaitReason reason, TStringBuf name) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        TTaskWaitDescription description;
        description.Reason = reason;
        description.Origin = ETaskWaitOrigin::Operator;
        description.OperatorId = operatorId;
        description.OperatorName = name;
        const bool spilling = reason == ETaskWaitReason::SpillRead || reason == ETaskWaitReason::SpillWrite;
        description.Condition = TTaskWaitCondition::Any({spilling
            ? TTaskDependencyVersion{SpillingDependency, SpillingVersion} : TTaskDependencyVersion{0, CallbackVersion}});
        ExplicitWaits.push_back(std::move(description));
    }
}

TTaskDependencyVersion TTaskSchedulingHandle::NewAsyncDependency() {
    std::lock_guard lock(Mutex);
    const auto id = NextAsyncDependency++;
    Y_ABORT_UNLESS(id < SpillingDependency);
    Versions.emplace(id, 0);
    return {id, 0};
}

void TTaskSchedulingHandle::Await(TTaskDependencyId dependency, ui64 version) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        ObservedWaits.push_back({dependency, version});
    }
}

void TTaskSchedulingHandle::MakeRunnable() {
    std::lock_guard lock(Mutex);
    ExplicitRunnable = true;
}

void TTaskSchedulingHandle::ReportWork(ETaskWorkKind work, ui64 operatorId, TStringBuf name, TStringBuf phase) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        Classification.Work = work;
        Classification.OperatorId = operatorId;
        Classification.OperatorName = name;
        Classification.Phase = phase;
    }
}

void TTaskSchedulingHandle::SealWait(ETaskWaitCoverage coverage) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        ProbeCoverage = coverage;
    }
}

void TTaskSchedulingHandle::SetOperatorProfile(TVector<TTaskOperatorTraits> operators) {
    std::lock_guard lock(Mutex);
    Operators = std::make_shared<const TVector<TTaskOperatorTraits>>(std::move(operators));
}

void TTaskSchedulingHandle::AwaitCallback() {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        ObservedWaits.push_back({0, CallbackVersion});
    }
}

bool TTaskSchedulingHandle::HasObservedWaits() const {
    std::lock_guard lock(Mutex);
    return !ObservedWaits.empty() || !ExplicitWaits.empty();
}

void TTaskSchedulingHandle::RecordInputBytes(ui64 bytes) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        InputBytes += bytes;
    }
}

void TTaskSchedulingHandle::RecordOutputBytes(ui64 bytes) {
    std::lock_guard lock(Mutex);
    if (State == EState::Running) {
        OutputBytes += bytes;
    }
}

TTaskRunProgress TTaskSchedulingHandle::GetRunProgressForTrace(TTaskRunProgress progress) const {
    std::lock_guard lock(Mutex);
    progress.InputBytes += InputBytes;
    progress.OutputBytes += OutputBytes;
    return progress;
}

std::optional<ui64> TTaskSchedulingHandle::TryDispatch() {
    std::lock_guard lock(Mutex);
    if (State != EState::Ready) {
        return {};
    }
    State = EState::Dispatched;
    ++Revision;
    if (Metrics) {
        ++*Metrics->Dispatched;
    }
    ++Generation;
    if (Trace) { Trace->State(SnapshotLocked()); }
    return Generation;
}

bool TTaskSchedulingHandle::Start(ui64 generation) {
    TTaskSchedulingSnapshot snapshot;
    {
        std::lock_guard lock(Mutex);
        if (State != EState::Dispatched || Generation != generation) {
            return false;
        }
        State = EState::Running;
        ++Revision;
        Continue = false;
        ExplicitRunnable = false;
        Classification = {};
        ProbeCoverage = ETaskWaitCoverage::Unknown;
        ObservedWaits.clear();
        ExplicitWaits.clear();
        CallbackVersion = Versions[0];
        SpillingVersion = Versions[SpillingDependency];
        AnyVersion = Versions[AnyDependency];
        // Old async subscriptions may complete later, but only dependencies
        // reported by this run can block it. Their tokens are never reused.
        for (auto it = Versions.begin(); it != Versions.end();) {
            if (it->first >= (ui64(1) << 63) && it->first != AnyDependency && it->first != SpillingDependency) {
                const auto previous = it++;
                Versions.erase(previous);
            } else {
                ++it;
            }
        }
        // A ready set can return Yield without probing a channel again.
        Wait = TTaskWaitCondition::Any({{AnyDependency, AnyVersion}});
        InputBytes = OutputBytes = 0;
        if (Metrics) {
            Metrics->ReadyWaitUs->Collect((TMonotonic::Now() - ReadySince).MicroSeconds());
        }
        if (Trace) {
            Trace->State(SnapshotLocked());
            NJson::TJsonValue args(NJson::JSON_MAP);
            args["ready_wait_us"] = (TMonotonic::Now() - ReadySince).MicroSeconds();
            args["generation"] = Generation;
            Trace->Event("started", TaskId, Actor, std::move(args));
        }
        if (ReportState) {
            snapshot = SnapshotLocked();
        }
    }
    if (ReportState) {
        ReportState(std::move(snapshot));
    }
    return true;
}

void TTaskSchedulingHandle::Complete(TTaskRunProgress progress, std::optional<TTaskWaitCondition> wait) {
    bool enqueue;
    TTaskSchedulingSnapshot snapshot;
    {
        std::lock_guard lock(Mutex);
        if (State != EState::Running) {
            return;
        }
        progress.InputBytes += InputBytes;
        progress.OutputBytes += OutputBytes;
        if (Metrics) {
            Metrics->Report(progress);
        }
        if (progress.RunnerInvoked) {
            Priority = progress.InputItems || progress.InputRows || progress.InputBytes ? ETaskPriority::Drain
                       : progress.OutputRows                                            ? ETaskPriority::Producer
                                                                                        : ETaskPriority::Normal;
        }
        // Runner completion may still need output finish confirmations. Old
        // input waits must not hide those notifications.
        if (progress.Finished && !wait) {
            ObservedWaits.clear();
            ExplicitWaits.clear();
        }
        LastWaits.clear();
        if (wait) {
            Wait = std::move(*wait);
        } else if (!ExplicitWaits.empty()) {
            Wait.AnyOf.clear();
            LastWaits = std::move(ExplicitWaits);
            for (const auto& description : LastWaits) {
                for (const auto& group : description.Condition.AnyOf) {
                    Wait.AnyOf.push_back(group);
                }
            }
        } else if (!ObservedWaits.empty()) {
            for (const auto& dependency : ObservedWaits) {
                TTaskWaitDescription description;
                description.Reason = dependency.Id && dependency.Id < (ui64(1) << 63)
                    ? (dependency.Id % 2 ? ETaskWaitReason::Input : ETaskWaitReason::Output) : ETaskWaitReason::Unknown;
                description.Origin = ETaskWaitOrigin::ChannelProbe;
                description.Precision = ETaskWaitPrecision::ObservedInputs;
                description.Condition = TTaskWaitCondition::Any({dependency});
                LastWaits.push_back(std::move(description));
            }
            Wait = TTaskWaitCondition::Any(std::move(ObservedWaits));
        }
        if (LastWaits.empty()) {
            TTaskWaitDescription description;
            description.Condition = Wait;
            LastWaits.push_back(std::move(description));
        }
        Classification.Coverage = wait ? ETaskWaitCoverage::Complete : ProbeCoverage;
        if (Classification.Coverage == ETaskWaitCoverage::Unknown
            && AnyOf(LastWaits, [](const auto& item) { return item.Reason != ETaskWaitReason::Unknown; })) {
            Classification.Coverage = ETaskWaitCoverage::Partial;
        }
        Classification.Readiness = AnyOf(LastWaits, [](const auto& item) { return item.Reason != ETaskWaitReason::Unknown; })
            ? ETaskReadiness::Waiting : ETaskReadiness::OpaqueYield;
        if (ExplicitRunnable || progress.BudgetExhausted) {
            Classification.Readiness = ETaskReadiness::Runnable;
            Classification.Coverage = ETaskWaitCoverage::Complete;
            if (Classification.Work == ETaskWorkKind::Unknown) { Classification.Work = ETaskWorkKind::Compute; }
        }
        if (progress.Finished || progress.QuotaRejected) {
            Classification.Work = ETaskWorkKind::Control;
            Classification.Readiness = ETaskReadiness::Waiting;
            if (!wait) { Classification.Coverage = ETaskWaitCoverage::Unknown; }
        }
        Classification.LegacyWakeups = Classification.Coverage != ETaskWaitCoverage::Complete;
        if (Classification.LegacyWakeups) {
            // An exact leaf future does not close the resume contract of an
            // opaque parent. Preserve both existing callback sources, including
            // notifications delivered between Fetch and parking.
            auto addFallback = [&](TTaskDependencyVersion fallback) {
                const bool present = AnyOf(Wait.AnyOf, [&](const auto& group) {
                    return group.size() == 1 && group[0].Id == fallback.Id && group[0].Version == fallback.Version;
                });
                if (!present) { Wait.AnyOf.push_back({fallback}); }
            };
            // An opaque parent may also have probed a different branch. These
            // are conservative alternatives, not a fabricated ALL predicate.
            for (auto dependency : ObservedWaits) { addFallback(dependency); }
            addFallback({0, CallbackVersion});
            addFallback({SpillingDependency, SpillingVersion});
        }
        LastProgress = progress;
        LastStopReason = progress.QuotaRejected ? ETaskStopReason::CpuQuota
            : progress.Finished ? ETaskStopReason::RunnerFinished
            : progress.BudgetExhausted ? ETaskStopReason::Budget
            : Continue || ExplicitRunnable ? ETaskStopReason::Runnable : ETaskStopReason::Waiting;
        State = EState::Blocked;
        ++Revision;
        enqueue = (Continue || ExplicitRunnable || progress.BudgetExhausted || Wait.IsSatisfied(Versions)) && MakeReadyLocked();
        if (Trace && !enqueue) { Trace->State(SnapshotLocked()); }
        if (ReportState) {
            snapshot = SnapshotLocked();
        }
    }
    if (ReportState) {
        ReportState(std::move(snapshot));
    }
    // Publish a runnable continuation before releasing its slot, so the next
    // dispatch compares it with the other ready tasks using the queue weights.
    if (enqueue) {
        Enqueue(shared_from_this());
    }
    Release();
}

void TTaskSchedulingHandle::Cancel(bool completed) {
    bool release;
    TTaskSchedulingSnapshot snapshot;
    {
        std::lock_guard lock(Mutex);
        if (State == EState::Cancelled || State == EState::Finished) {
            return;
        }
        release = State == EState::Running || State == EState::Dispatched;
        State = completed ? EState::Finished : EState::Cancelled;
        LastStopReason = completed ? ETaskStopReason::RunnerFinished : ETaskStopReason::Cancelled;
        ++Revision;
        if (Trace) { Trace->State(SnapshotLocked()); }
        if (ReportState) {
            snapshot = SnapshotLocked();
        }
    }
    if (ReportState) {
        ReportState(std::move(snapshot));
    }
    if (release) {
        Release();
    }
}

ETaskPriority TTaskSchedulingHandle::GetPriority() const {
    std::lock_guard lock(Mutex);
    return Priority;
}

TTaskSchedulingSnapshot TTaskSchedulingHandle::SnapshotLocked() const {
    TTaskSchedulingSnapshot snapshot{.Actor = Actor, .Query = Query, .TaskId = TaskId,
        .Revision = Revision, .Generation = Generation, .State = State, .Priority = Priority,
        .Progress = LastProgress, .Waits = LastWaits, .StopReason = LastStopReason,
        .Classification = Classification, .ResumeCondition = Wait, .Operators = Operators};
    auto& classification = snapshot.Classification;
    if (State == EState::Finished || State == EState::Cancelled) {
        classification.Readiness = State == EState::Finished ? ETaskReadiness::Finished : ETaskReadiness::Cancelled;
        classification.Work = ETaskWorkKind::Control;
        classification.Coverage = ETaskWaitCoverage::Complete;
        classification.LegacyWakeups = false;
    } else if (State == EState::Ready || State == EState::Dispatched) {
        if (LastStopReason == ETaskStopReason::None) {
            classification.Readiness = ETaskReadiness::Runnable;
            classification.Work = ETaskWorkKind::Control;
            classification.Coverage = ETaskWaitCoverage::Complete;
            classification.LegacyWakeups = false;
        } else if (classification.Readiness != ETaskReadiness::Runnable) {
            const bool exact = classification.Coverage == ETaskWaitCoverage::Complete && !LastWaits.empty()
                && AllOf(LastWaits, [](const auto& item) { return item.Precision == ETaskWaitPrecision::Exact; });
            classification.Readiness = exact && Wait.IsSatisfied(Versions) ? ETaskReadiness::Runnable : ETaskReadiness::Recheck;
        }
    }
    return snapshot;
}

TTaskSchedulingSnapshot TTaskSchedulingHandle::GetSnapshot() const {
    std::lock_guard lock(Mutex);
    return SnapshotLocked();
}

namespace {

struct TEvTaskReady : NActors::TEventLocal<TEvTaskReady, TDqComputeEvents::EvTaskReady> {
    explicit TEvTaskReady(std::shared_ptr<TTaskSchedulingHandle> task) : Task(std::move(task)) {}
    std::shared_ptr<TTaskSchedulingHandle> Task;
};

struct TEvTaskReleased : NActors::TEventLocal<TEvTaskReleased, TDqComputeEvents::EvTaskReleased> {};

struct TEvRegisterGraph : NActors::TEventLocal<TEvRegisterGraph, TDqComputeEvents::EvTaskRegisterGraph> {
    TEvRegisterGraph(NActors::TActorId query, ui64 registration, std::shared_ptr<const TQueryTaskGraph> graph, TVector<ui64> localTasks)
        : Query(query), Registration(registration), Graph(std::move(graph)), LocalTasks(std::move(localTasks)) {}
    NActors::TActorId Query;
    ui64 Registration;
    std::shared_ptr<const TQueryTaskGraph> Graph;
    TVector<ui64> LocalTasks;
};

struct TEvForgetGraph : NActors::TEventLocal<TEvForgetGraph, TDqComputeEvents::EvTaskForgetGraph> {
    TEvForgetGraph(NActors::TActorId query, ui64 registration) : Query(query), Registration(registration) {}
    NActors::TActorId Query;
    ui64 Registration;
};

struct TEvTaskRegistered : NActors::TEventLocal<TEvTaskRegistered, TDqComputeEvents::EvTaskRegistered> {
    TEvTaskRegistered(ui64 registration, TTaskSchedulingSnapshot state) : Registration(registration), State(std::move(state)) {}
    ui64 Registration;
    TTaskSchedulingSnapshot State;
};

struct TEvTaskStateUpdate : NActors::TEventLocal<TEvTaskStateUpdate, TDqComputeEvents::EvTaskStateUpdate> {
    TEvTaskStateUpdate(ui64 registration, TTaskSchedulingSnapshot state) : Registration(registration), State(std::move(state)) {}
    ui64 Registration;
    TTaskSchedulingSnapshot State;
};

struct TEvDescribeQuery : NActors::TEventLocal<TEvDescribeQuery, TDqComputeEvents::EvTaskDescribeQuery> {
    TEvDescribeQuery(NActors::TActorId query, NActors::TActorId recipient) : Query(query), Recipient(recipient) {}
    NActors::TActorId Query, Recipient;
};

class TTaskDispatcher : public NActors::TActorBootstrapped<TTaskDispatcher> {
public:
    explicit TTaskDispatcher(ui32 maxInFlight) : MaxInFlight(maxInFlight) {}
    void Bootstrap() { Become(&TTaskDispatcher::StateWork); }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTaskReady, Handle);
            hFunc(TEvRegisterGraph, Handle);
            hFunc(TEvForgetGraph, Handle);
            hFunc(TEvTaskRegistered, Handle);
            hFunc(TEvTaskStateUpdate, Handle);
            hFunc(TEvDescribeQuery, Handle);
            cFunc(TEvTaskReleased::EventType, Released);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    void Handle(TEvTaskReady::TPtr& ev) {
        auto task = std::move(ev->Get()->Task);
        if (task->TaskId) {
            UpdateTask(task->GetSnapshot());
        }
        Queues[static_cast<ui32>(task->GetPriority())].push_back(std::move(task));
        Dispatch();
    }
    void Handle(TEvRegisterGraph::TPtr& ev) {
        const auto& message = *ev->Get();
        auto& query = Queries[message.Query];
        if (query.Registration > message.Registration) {
            return;
        }
        if (query.Registration != message.Registration) {
            query = {};
            query.Registration = message.Registration;
        }
        query.Graph = message.Graph;
        for (ui64 taskId : message.LocalTasks) {
            Y_ABORT_UNLESS(query.Graph->FindTask(taskId));
            query.LocalTasks.insert(taskId);
        }
    }
    void Handle(TEvForgetGraph::TPtr& ev) {
        auto it = Queries.find(ev->Get()->Query);
        if (it != Queries.end() && it->second.Registration == ev->Get()->Registration) {
            Queries.erase(it);
        }
    }
    void Handle(TEvTaskRegistered::TPtr& ev) {
        const auto& message = *ev->Get();
        auto it = Queries.find(message.State.Query);
        if (it != Queries.end() && it->second.Registration == message.Registration) {
            it->second.ManagedTasks[message.State.TaskId] = message.State;
        }
    }
    void Handle(TEvTaskStateUpdate::TPtr& ev) {
        auto it = Queries.find(ev->Get()->State.Query);
        if (it != Queries.end() && it->second.Registration == ev->Get()->Registration) {
            UpdateTask(ev->Get()->State);
        }
    }
    void UpdateTask(const TTaskSchedulingSnapshot& state) {
        auto query = Queries.find(state.Query);
        if (query == Queries.end()) {
            return;
        }
        auto task = query->second.ManagedTasks.find(state.TaskId);
        // A late report from a replaced actor must not overwrite its successor.
        if (task != query->second.ManagedTasks.end() && task->second.Actor == state.Actor
            && task->second.Revision <= state.Revision) {
            task->second = state;
        }
    }
    void Handle(TEvDescribeQuery::TPtr& ev) {
        TQueryGraphSnapshot result;
        result.Query = ev->Get()->Query;
        result.NodeId = SelfId().NodeId();
        auto it = Queries.find(result.Query);
        if (it != Queries.end()) {
            result.Graph = it->second.Graph;
            for (ui64 id : it->second.LocalTasks) {
                result.LocalTaskIds.push_back(id);
            }
            for (const auto& [id, state] : it->second.ManagedTasks) {
                result.ManagedTasks.push_back(state);
            }
            Sort(result.LocalTaskIds);
            Sort(result.ManagedTasks, [](const auto& left, const auto& right) { return left.TaskId < right.TaskId; });
        }
        Send(ev->Get()->Recipient, new TEvQueryGraph(std::move(result)));
    }
    void Released() {
        Y_ABORT_UNLESS(InFlight);
        --InFlight;
        Dispatch();
    }
    void Dispatch() {
        // Weighted round robin guarantees producer tasks a turn even under a
        // continuous drain workload. FIFO within each class prevents newer tasks overtaking older ones.
        static constexpr ui32 Order[] = {0, 0, 0, 0, 1, 1, 2};
        while (InFlight < MaxInFlight) {
            std::shared_ptr<TTaskSchedulingHandle> task;
            for (ui32 attempt = 0; attempt < std::size(Order); ++attempt) {
                auto& queue = Queues[Order[Cursor++ % std::size(Order)]];
                if (!queue.empty()) {
                    task = std::move(queue.front());
                    queue.pop_front();
                    break;
                }
            }
            if (!task) {
                return;
            }
            if (auto generation = task->TryDispatch()) {
                if (task->TaskId) {
                    UpdateTask(task->GetSnapshot());
                }
                ++InFlight;
                Send(task->Actor, new TEvTaskRunPermit(task, *generation));
            }
        }
    }

    const ui32 MaxInFlight;
    ui32 InFlight = 0;
    ui64 Cursor = 0;
    std::array<std::deque<std::shared_ptr<TTaskSchedulingHandle>>, 3> Queues;
    struct TQueryState {
        ui64 Registration = 0;
        std::shared_ptr<const TQueryTaskGraph> Graph;
        THashSet<ui64> LocalTasks;
        THashMap<ui64, TTaskSchedulingSnapshot> ManagedTasks;
    };
    THashMap<NActors::TActorId, TQueryState> Queries;
};

class TTaskScheduler : public IDqTaskScheduler, public std::enable_shared_from_this<TTaskScheduler> {
public:
    TTaskScheduler(NActors::TActorSystem* actorSystem, ui32 poolId, NMonitoring::TDynamicCounterPtr counters,
                   TTaskSchedulerSettings settings)
        : ActorSystem(actorSystem), Settings(settings), Metrics(std::make_shared<TTaskSchedulerMetrics>(counters)),
          Traces(actorSystem->NodeId, settings.Trace) {
        Y_ENSURE(Settings.Shards && Settings.MaxInFlightPerShard);
        Y_ENSURE(Settings.WorkerSliceCpuUs && Settings.WorkerActivationUs);
        if (!Settings.ObserveOnly && Settings.UseWorkerPool) {
            WorkPool = std::make_shared<TTaskWorkPool>(Settings.MaxInFlightPerShard,
                [actorSystem](auto worker) { WakeDqComputeWorker(actorSystem, worker); }, counters);
            StartDqComputeWorkers(actorSystem, poolId, WorkPool, Settings);
        }
        for (ui32 i = 0; !Settings.ObserveOnly && !Settings.UseWorkerPool && i < Settings.Shards; ++i) {
            Dispatchers.push_back(
                ActorSystem->Register(new TTaskDispatcher(Settings.MaxInFlightPerShard), NActors::TMailboxType::HTSwap, poolId));
        }
    }

    std::shared_ptr<const TQueryGraphRegistration> RegisterGraph(NActors::TActorId query,
        std::shared_ptr<const TQueryTaskGraph> graph, TVector<ui64> localTasks) override
    {
        if (Settings.ObserveOnly && !Settings.Trace.Enabled) {
            return {};
        }
        Y_ENSURE(graph);
        for (ui64 id : localTasks) {
            Y_ENSURE(graph->FindTask(id), "Local task is absent from the query graph");
        }
        const auto dispatcher = WorkPool || Settings.ObserveOnly ? NActors::TActorId{} : GetDispatcher(query);
        std::shared_ptr<const TQueryGraphRegistration> registration;
        ui64 id;
        {
            std::lock_guard lock(RegistrationsMutex);
            auto& entry = Registrations[query];
            registration = entry.Registration.lock();
            if (!registration) {
                entry.Id = ++NextRegistration;
                const ui64 registrationId = entry.Id;
                auto trace = Traces.GetOrCreate(query);
                if (trace) { trace->Graph(*graph, localTasks); }
                registration = std::make_shared<TQueryGraphRegistration>(std::move(graph),
                    [weak = weak_from_this(), query, dispatcher, registrationId, trace] {
                        if (trace) { trace->Finish(); }
                        if (auto self = weak.lock()) {
                            {
                                std::lock_guard lock(self->RegistrationsMutex);
                                auto it = self->Registrations.find(query);
                                if (it != self->Registrations.end() && it->second.Id == registrationId) {
                                    self->Registrations.erase(it);
                                }
                            }
                            if (self->WorkPool) {
                                self->WorkPool->ForgetGraph(query, registrationId);
                            } else if (!self->Settings.ObserveOnly) {
                                self->ActorSystem->Send(dispatcher, new TEvForgetGraph(query, registrationId));
                            }
                        }
                    }, trace);
                entry.Registration = registration;
            }
            id = entry.Id;
        }
        if (const auto& trace = registration->Trace) {
            NJson::TJsonValue args(NJson::JSON_MAP), local(NJson::JSON_ARRAY);
            for (ui64 task : localTasks) { local.AppendValue(ToString(task)); }
            args["local_tasks"] = std::move(local);
            trace->Event("local_tasks_registered", 0, {}, std::move(args));
        }
        if (WorkPool) {
            WorkPool->RegisterGraph(query, id, registration->Graph, localTasks);
        } else if (!Settings.ObserveOnly) {
            ActorSystem->Send(dispatcher, new TEvRegisterGraph(query, id, registration->Graph, std::move(localTasks)));
        }
        return registration;
    }

    std::shared_ptr<TTaskSchedulingHandle> Register(NActors::TActorId actor, NActors::TActorId query, ui64 taskId) override {
        if (Settings.ObserveOnly) {
            return RegisterObserver(actor, query, taskId);
        }
        const auto dispatcher = WorkPool || Settings.ObserveOnly ? NActors::TActorId{} : GetDispatcher(query);
        std::shared_ptr<const TQueryGraphRegistration> registration;
        ui64 registrationId = 0;
        {
            std::lock_guard lock(RegistrationsMutex);
            auto it = Registrations.find(query);
            if (it != Registrations.end()) {
                registration = it->second.Registration.lock();
                registrationId = registration ? it->second.Id : 0;
            }
        }
        auto* actorSystem = ActorSystem;
        if (registration && taskId) {
            Y_ENSURE(registration->Graph->FindTask(taskId));
        }
        auto trace = registration ? registration->Trace : Traces.GetOrCreate(query);
        if (WorkPool) {
            auto pool = WorkPool;
            auto task = std::make_shared<TTaskSchedulingHandle>(actor, query,
                [pool](auto task) { pool->Enqueue(std::move(task)); }, [] {}, Metrics, false, taskId,
                [pool, registrationId](auto state) { pool->Report(registrationId, std::move(state)); }, std::move(registration), std::move(trace));
            pool->RegisterTask(task);
            return task;
        }
        TTaskSchedulingHandle::TStateReporter report;
        if (registration && taskId) {
            Y_ENSURE(registration->Graph->FindTask(taskId));
            report = [actorSystem, dispatcher, registrationId](auto state) {
                actorSystem->Send(dispatcher, new TEvTaskStateUpdate(registrationId, std::move(state)));
            };
        }
        auto task = std::make_shared<TTaskSchedulingHandle>(
            actor, query, [actorSystem, dispatcher](auto task) { actorSystem->Send(dispatcher, new TEvTaskReady(std::move(task))); },
            [actorSystem, dispatcher]() { actorSystem->Send(dispatcher, new TEvTaskReleased()); }, Metrics, false, taskId,
            std::move(report), std::move(registration), std::move(trace));
        if (registrationId && taskId) {
            ActorSystem->Send(dispatcher, new TEvTaskRegistered(registrationId, task->GetSnapshot()));
        }
        return task;
    }

    std::shared_ptr<TTaskSchedulingHandle> RegisterObserver(NActors::TActorId actor, NActors::TActorId query, ui64 taskId) override {
        std::shared_ptr<const TQueryGraphRegistration> registration;
        {
            std::lock_guard lock(RegistrationsMutex);
            if (auto it = Registrations.find(query); it != Registrations.end()) {
                registration = it->second.Registration.lock();
            }
        }
        auto trace = registration ? registration->Trace : Traces.GetOrCreate(query);
        return std::make_shared<TTaskSchedulingHandle>(actor, query, [](auto) {}, [] {}, Metrics, true, taskId,
            TTaskSchedulingHandle::TStateReporter{}, std::move(registration), std::move(trace));
    }

    TString ExportTrace(TStringBuf query) override { return Traces.Export(query); }

    void DescribeQuery(NActors::TActorId query, NActors::TActorId recipient) override {
        if (Settings.ObserveOnly) {
            TQueryGraphSnapshot result;
            result.Query = query;
            ActorSystem->Send(recipient, new TEvQueryGraph(std::move(result)));
            return;
        }
        if (WorkPool) {
            ActorSystem->Send(recipient, new TEvQueryGraph(WorkPool->Describe(query, ActorSystem->NodeId)));
        } else {
            ActorSystem->Send(GetDispatcher(query), new TEvDescribeQuery(query, recipient));
        }
    }

    void BindExecution(NActors::TActorId query, NActors::TActorId actor, std::shared_ptr<IDqTaskExecution> execution) override {
        Y_ENSURE(WorkPool);
        WorkPool->BindExecution(query, actor, std::move(execution));
    }
    void NotifyExecutionAvailable() override {
        if (WorkPool) {
            WorkPool->NotifyAvailable();
        }
    }
    void SetQueryWeight(NActors::TActorId query, double weight) override {
        Y_ENSURE(WorkPool);
        WorkPool->SetQueryWeight(query, weight);
    }

    const TTaskSchedulerSettings& GetSettings() const override { return Settings; }

private:
    NActors::TActorId GetDispatcher(NActors::TActorId query) const {
        return Dispatchers[THash<NActors::TActorId>{}(query) % Dispatchers.size()];
    }
    NActors::TActorSystem* const ActorSystem;
    const TTaskSchedulerSettings Settings;
    const std::shared_ptr<TTaskSchedulerMetrics> Metrics;
    TTaskTraceRegistry Traces;
    TVector<NActors::TActorId> Dispatchers;
    std::shared_ptr<TTaskWorkPool> WorkPool;
    struct TRegistration {
        ui64 Id = 0;
        std::weak_ptr<const TQueryGraphRegistration> Registration;
    };
    std::mutex RegistrationsMutex;
    THashMap<NActors::TActorId, TRegistration> Registrations;
    ui64 NextRegistration = 0;
};

} // namespace

std::shared_ptr<IDqTaskScheduler> CreateDqTaskScheduler(NActors::TActorSystem* actorSystem, ui32 poolId,
                                                        NMonitoring::TDynamicCounterPtr counters, const TTaskSchedulerSettings& settings) {
    return std::make_shared<TTaskScheduler>(actorSystem, poolId, counters, settings);
}

} // namespace NYql::NDq
