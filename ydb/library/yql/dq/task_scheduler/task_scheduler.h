#pragma once

#include "task_graph.h"
#include "task_trace.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace NYql::NDq {

// Dependencies are local to a task. Zero is reserved for explicit continuations
// and callbacks whose precise dependency has not been instrumented yet.
using TTaskDependencyId = ui64;
inline TTaskDependencyId InputDependency(ui64 channel) { return channel * 2 + 1; }
inline TTaskDependencyId OutputDependency(ui64 channel) { return channel * 2 + 2; }
inline constexpr TTaskDependencyId SpillingDependency = ~ui64(0) - 1;

struct TTaskDependencyVersion {
    TTaskDependencyId Id = 0;
    ui64 Version = 0;
};

struct TTaskWaitCondition {
    // OR of AND groups. A term is satisfied when its version changes.
    TVector<TVector<TTaskDependencyVersion>> AnyOf;

    static TTaskWaitCondition Any(TVector<TTaskDependencyVersion> dependencies);
    static TTaskWaitCondition All(TVector<TTaskDependencyVersion> dependencies);
    bool IsSatisfied(const THashMap<TTaskDependencyId, ui64>& versions) const;
};

enum class ETaskPriority : ui32 {
    Drain,
    Normal,
    Producer,
};

struct TTaskRunProgress {
    ui64 InputItems = 0;
    ui64 InputRows = 0;
    ui64 InputBytes = 0;
    ui64 OutputRows = 0;
    ui64 OutputBytes = 0;
    ui64 CpuTimeUs = 0;
    ui64 ElapsedTimeUs = 0;
    bool RunnerInvoked = false;
    bool QuotaRejected = false;
    bool Finished = false;
    bool BudgetExhausted = false;

    bool HasObservedProgress() const { return InputItems || InputRows || InputBytes || OutputRows || OutputBytes || Finished; }
};

enum class ETaskWaitReason { Unknown, Input, Output, SpillRead, SpillWrite, AsyncIo };
enum class ETaskInputSide { None, Left, Right, Build, Probe };
enum class ETaskWaitOrigin { ChannelProbe, Operator, OpaqueFallback };
enum class ETaskWaitPrecision { Exact, ObservedInputs, Opaque };

struct TTaskInputContext {
    ui64 OperatorId = 0;
    ETaskInputSide Side = ETaskInputSide::None;
};

struct TTaskWaitDescription {
    ETaskWaitReason Reason = ETaskWaitReason::Unknown;
    ETaskWaitOrigin Origin = ETaskWaitOrigin::OpaqueFallback;
    ETaskWaitPrecision Precision = ETaskWaitPrecision::Opaque;
    ui64 OperatorId = 0;
    ETaskInputSide InputSide = ETaskInputSide::None;
    TTaskWaitCondition Condition;
    TVector<TTaskInputContext> InputPath;
};

enum class ETaskStopReason { None, Waiting, Runnable, Budget, RunnerFinished, CpuQuota, Cancelled };

enum class ETaskSchedulingState { Blocked, Ready, Dispatched, Running, Cancelled, Finished };

struct TTaskSchedulingSnapshot {
    NActors::TActorId Actor, Query;
    ui64 TaskId = 0;
    ui64 Revision = 0;
    ui64 Generation = 0;
    ETaskSchedulingState State = ETaskSchedulingState::Blocked;
    ETaskPriority Priority = ETaskPriority::Normal;
    TTaskRunProgress Progress;
    TVector<TTaskWaitDescription> Waits;
    ETaskStopReason StopReason = ETaskStopReason::None;
};

// Kept by the planner/query manager, local compute actors and scheduling handles.
// The last owner unregisters this node's graph, including failed task starts.
class TQueryGraphRegistration {
public:
    TQueryGraphRegistration(std::shared_ptr<const TQueryTaskGraph> graph, std::function<void()> release,
        std::shared_ptr<TQueryTaskTrace> trace = {})
        : Graph(std::move(graph)), Trace(std::move(trace)), Release(std::move(release)) {}
    ~TQueryGraphRegistration() { Release(); }
    TQueryGraphRegistration(const TQueryGraphRegistration&) = delete;
    TQueryGraphRegistration& operator=(const TQueryGraphRegistration&) = delete;
    const std::shared_ptr<const TQueryTaskGraph> Graph;
    const std::shared_ptr<TQueryTaskTrace> Trace;
private:
    const std::function<void()> Release;
};

struct TQueryGraphSnapshot {
    NActors::TActorId Query;
    ui32 NodeId = 0;
    std::shared_ptr<const TQueryTaskGraph> Graph;
    TVector<ui64> LocalTaskIds;
    TVector<TTaskSchedulingSnapshot> ManagedTasks;
};

struct TTaskSchedulerSettings {
    TTaskTraceSettings Trace;
    bool ObserveOnly = false;
    ui32 Shards = 4;
    ui32 MaxInFlightPerShard = 2;
    // Zero disables cooperative runner yields, including CPU/deadline yields.
    ui64 MaxRowsPerRun = 0;
    bool UseWorkerPool = false;
    ui64 WorkerSliceCpuUs = 1000;
    ui64 WorkerActivationUs = 2000;
};

struct TTaskSchedulerMetrics;
class IDqTaskExecution;

class TTaskSchedulingHandle : public std::enable_shared_from_this<TTaskSchedulingHandle> {
public:
    using TEnqueue = std::function<void(std::shared_ptr<TTaskSchedulingHandle>)>;
    using TRelease = std::function<void()>;
    using TStateReporter = std::function<void(TTaskSchedulingSnapshot)>;

    TTaskSchedulingHandle(NActors::TActorId actor, NActors::TActorId query, TEnqueue enqueue, TRelease release, std::shared_ptr<TTaskSchedulerMetrics> metrics = {},
                          bool observeOnly = false, ui64 taskId = 0, TStateReporter reportState = {},
                          std::shared_ptr<const TQueryGraphRegistration> graphRegistration = {},
                          std::shared_ptr<TQueryTaskTrace> trace = {});

    void RequestExecution();
    void Signal(TTaskDependencyId dependency, ETaskPriority priority = ETaskPriority::Normal);
    ui64 Observe(TTaskDependencyId dependency);
    // The owner calls these around a single Fetch attempt. Intermediate failed
    // probes must not leak into the next successful Fetch's wait description.
    void BeginProbe();
    void Await(TTaskDependencyId dependency, ui64 version);
    void AwaitCallback();
    std::pair<size_t, size_t> BeginInputWait() const;
    void ReportInputWait(std::pair<size_t, size_t> scope, ui64 operatorId, ETaskInputSide side);
    void ReportWait(TTaskWaitDescription description);
    void ReportCallbackWait(ui64 operatorId, ETaskWaitReason reason);
    TTaskDependencyVersion NewAsyncDependency();
    bool HasObservedWaits() const;
    void MakeRunnable();
    void RecordInputBytes(ui64 bytes);
    void RecordOutputBytes(ui64 bytes);
    TTaskRunProgress GetRunProgressForTrace(TTaskRunProgress progress) const;

    std::optional<ui64> TryDispatch();
    bool Start(ui64 generation);
    void Complete(TTaskRunProgress progress, std::optional<TTaskWaitCondition> wait = {});
    void Cancel(bool completed = false);

    ETaskPriority GetPriority() const;
    TTaskSchedulingSnapshot GetSnapshot() const;
    const NActors::TActorId Actor;
    const NActors::TActorId Query;
    const ui64 TaskId;
    const bool ObserveOnly;
    const std::shared_ptr<TQueryTaskTrace> Trace;

private:
    using EState = ETaskSchedulingState;
    bool MakeReadyLocked();
    TTaskSchedulingSnapshot SnapshotLocked() const;

    const TEnqueue Enqueue;
    const TRelease Release;
    const std::shared_ptr<TTaskSchedulerMetrics> Metrics;
    const TStateReporter ReportState;
    const std::shared_ptr<const TQueryGraphRegistration> GraphRegistration;
    mutable std::mutex Mutex;
    EState State = EState::Blocked;
    ETaskPriority Priority = ETaskPriority::Normal;
    THashMap<TTaskDependencyId, ui64> Versions;
    TTaskWaitCondition Wait;
    TVector<TTaskDependencyVersion> ObservedWaits;
    TVector<TTaskWaitDescription> ExplicitWaits, LastWaits;
    TTaskRunProgress LastProgress;
    ETaskStopReason LastStopReason = ETaskStopReason::None;
    ui64 Revision = 0;
    ui64 NextAsyncDependency = ui64(1) << 63;
    ui64 Generation = 0;
    ui64 CallbackVersion = 0;
    ui64 SpillingVersion = 0;
    ui64 AnyVersion = 0;
    bool Continue = false;
    ui64 InputBytes = 0;
    ui64 OutputBytes = 0;
    TMonotonic ReadySince;
};

struct TEvTaskRunPermit : NActors::TEventLocal<TEvTaskRunPermit, TDqComputeEvents::EvTaskRunPermit> {
    TEvTaskRunPermit(std::shared_ptr<TTaskSchedulingHandle> task, ui64 generation) : Task(std::move(task)), Generation(generation) {}
    std::shared_ptr<TTaskSchedulingHandle> Task;
    ui64 Generation;
};

struct TEvQueryGraph : NActors::TEventLocal<TEvQueryGraph, TDqComputeEvents::EvTaskQueryGraph> {
    explicit TEvQueryGraph(TQueryGraphSnapshot graph) : Snapshot(std::move(graph)) {}
    TQueryGraphSnapshot Snapshot;
};

class IDqTaskScheduler {
public:
    virtual ~IDqTaskScheduler() = default;
    // Query identifies one execution graph (the KQP executer actor). All its
    // tasks on this node share one query queue (or a dispatcher in baseline mode).
    virtual std::shared_ptr<const TQueryGraphRegistration> RegisterGraph(NActors::TActorId query,
        std::shared_ptr<const TQueryTaskGraph> graph, TVector<ui64> localTaskIds) = 0;
    virtual std::shared_ptr<TTaskSchedulingHandle> Register(NActors::TActorId actor, NActors::TActorId query, ui64 taskId = 0) = 0;
    // Trace-only registration preserves the original task-bound execution path.
    virtual std::shared_ptr<TTaskSchedulingHandle> RegisterObserver(NActors::TActorId, NActors::TActorId, ui64) { return {}; }
    virtual TString ExportTrace(TStringBuf = {}) { return "{\"enabled\":false,\"queries\":[]}"; }
    virtual void BindExecution(NActors::TActorId query, NActors::TActorId actor, std::shared_ptr<IDqTaskExecution> execution) = 0;
    virtual void NotifyExecutionAvailable() = 0;
    virtual void SetQueryWeight(NActors::TActorId query, double weight) = 0;
    // Replies asynchronously with TEvQueryGraph. Worker mode copies metadata
    // under its queue lock; baseline mode asks the owning dispatcher actor.
    virtual void DescribeQuery(NActors::TActorId query, NActors::TActorId recipient) = 0;
    virtual const TTaskSchedulerSettings& GetSettings() const = 0;
};

std::shared_ptr<IDqTaskScheduler> CreateDqTaskScheduler(NActors::TActorSystem* actorSystem, ui32 poolId,
                                                        NMonitoring::TDynamicCounterPtr counters, const TTaskSchedulerSettings& settings);

} // namespace NYql::NDq
