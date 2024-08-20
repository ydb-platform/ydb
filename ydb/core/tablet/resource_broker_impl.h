#pragma once

#include "resource_broker.h"

#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NResourceBroker {

class TResourceQueue;
using TResourceQueuePtr = TIntrusivePtr<TResourceQueue>;
class TTaskQueue;
using TTaskQueuePtr = TIntrusivePtr<TTaskQueue>;
class TTaskCounters;
using TTaskCountersPtr = TIntrusivePtr<TTaskCounters>;

/**
 * Resource limit class. Limits are attached to queues to limit resource
 * consumption within a single queue. Also used to track total resource
 * consumption.
 */
class TResourceLimit : public TThrRefBase {
public:
    TResourceLimit(const NKikimrResourceBroker::TResources &limit);

    bool HasResources(const TResourceValues &values) const;
    void HoldResources(const TResourceValues &values);
    void ReleaseResources(const TResourceValues &values);

public:
    TResourceValues Limit;
    TResourceValues Used;
};
using TResourceLimitPtr = TIntrusivePtr<TResourceLimit>;
using TResourceLimitConstPtr = TIntrusiveConstPtr<TResourceLimit>;

/**
 * Class to describe task submitted to scheduler.
 */
class TTask : public TThrRefBase,
              public TEvResourceBroker::TTask {
public:
    TTask(const TEvResourceBroker::TTask &task, const TActorId &client,
          TInstant timestamp, TTaskCountersPtr counters);

    // Get string with task's name, ID and client info.
    TString GetIdString() const;

    // Debug print of current state into specified stream.
    void OutputState(IOutputStream &os, const TString &prefix) const;

    // Returns the mask of required resources
    ui64 GetRequiredResourcesMask() const {
        ui64 mask = 0;
        for (size_t i = 0; i < RESOURCE_COUNT; ++i) {
            if (RequiredResources[i]) {
                mask |= ui64(1) << i;
            }
        }
        return mask;
    }

public:
    // Actor which submitted task.
    TActorId Client;
    // Task queue task is currently attached to.
    TTaskQueuePtr Queue;
    // True if task has allocated resources.
    bool InFly;
    // When task was submitted.
    TInstant SubmitTime;
    // When task got resources.
    TInstant StartTime;
    // When task is is supposed to finish.
    TInstant FinishTime;
    // Task name used for logs.
    TString IdString;
    // Counters affected by task.
    TTaskCountersPtr Counters;
};
using TTaskPtr = TIntrusivePtr<TTask>;

/**
 * Class to hold common counters tracked per queue and per task type.
 */
class TBaseCounters : public TThrRefBase {
public:
    TBaseCounters() = default;
    TBaseCounters(const ::NMonitoring::TDynamicCounterPtr &counters);
    TBaseCounters(const TBaseCounters &other) = default;
    TBaseCounters(TBaseCounters &&other) = default;

    TBaseCounters &operator=(const TBaseCounters &other) = default;
    TBaseCounters &operator=(TBaseCounters &&other) = default;

    void HoldResources(const TResourceValues &values);
    void ReleaseResources(const TResourceValues &values);

public:
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, RESOURCE_COUNT> Consumption;
    ::NMonitoring::TDynamicCounters::TCounterPtr FinishedTasks;
    ::NMonitoring::TDynamicCounters::TCounterPtr EnqueuedTasks;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyTasks;
};

/**
 * Class to hold counters tracked per queue. Also used to track total counters.
 */
class TQueueCounters : public TBaseCounters {
public:
    TQueueCounters(const ::NMonitoring::TDynamicCounterPtr &counters);
};
using TQueueCountersPtr = TIntrusivePtr<TQueueCounters>;

/**
 * Class to hold counters tracked per task type.
 */
class TTaskCounters : public TBaseCounters {
public:
    TTaskCounters() = default;
    TTaskCounters(const ::NMonitoring::TDynamicCounterPtr &counters);
    TTaskCounters(const TTaskCounters &other) = default;
    TTaskCounters(TTaskCounters &&other) = default;

    TTaskCounters &operator=(const TTaskCounters &other) = default;
    TTaskCounters &operator=(TTaskCounters &&other) = default;
};

/**
 * Class to hold duration statistics used to track average execution
 * time for tasks.
 */
class TDurationStat {
public:
    TDurationStat(TDuration def, size_t history);
    TDurationStat(const TDurationStat &other) = default;
    TDurationStat(TDurationStat &&other) = default;

    TDurationStat &operator=(const TDurationStat &other) = default;
    TDurationStat &operator=(TDurationStat &&other) = default;

    void Add(TDuration duration);

    TDuration GetAverage() const;

private:
    TVector<TDuration> Values;
    size_t Current;
    TDuration Total;
};

/**
 * Queue of scheduled tasks. Primary tasks order is specified by their priority
 * (lower priority value means earlier execution). Secondary tasks order is
 * specified by timestamp (FIFO).
 *
 * Tasks are stored in a heap. Heap is modified only when tasks are inserted
 * and removed from the heap. Therefore task modification should be done only
 * after removal from its queue (possibly with following insertion back).
 *
 * Each queue has attached resource limit usage, reference to total resource
 * usage limit, queue counters and total counters. Queue reflects its tasks
 * state and resource usage in all limits and counters attached to queue and
 * tasks.
 *
 * References to in-fly tasks are not stored in tasks queue but such tasks
 * still should be attached to queue using InsertTask method to get correct
 * counters, limits and usage work. Therefore resource allocation for task
 * is done in several steps:
 *    queue->PopTask();
 *    set task->InFly;
 *    set task->StartTime;
 *    set task->FinishTime; (estimated)
 *    queue->InsertTask(task)
 * When InFly task is attached to queue planned resource usage is updated
 * according to resources and execution time required by the task.
 *
 * Planned resource usage is the first parameter used to schedule resource
 * allocation. Planned resource usage is modified on task insertion as
 *    Up += Rd * Te / W
 * where
 *  W  - Queue weight.
 *  Te - Estimated task execution time. Execution time is estimated basing
 *       execution statistics for tasks of the same type.
 *  Rd - Dominant component of task's required resources. Dominant component
 *       is computed according to current queue resource consumption and total
 *       available resources (find max used[RES]/total[RES]).
 *
 * Planned resource usage is corrected in case task execution is finished before
 * its estimated FinishTime.
 *
 * Real resource usage is the second parameter used to schedule resource
 * allocation. It is computed before each queue resource consumption modification
 * and also may be additionally re-computed before choosing the next task by
 * scheduler.
 *
 * Real resource usage may exceed planned one. Therefore max of these two components
 * is used when queue with the most/least usage is chosen. Also planned resource
 * usage must be corrected before modifications:
 *    Up_new = Max(Up_old, Ur_old) + Rd * Te / W
 */
class TTaskQueue : public TThrRefBase {
private:
    struct TTaskEarlier {
        bool operator()(const TTaskPtr &l, const TTaskPtr &r) const;
    };

public:
    TTaskQueue(const NKikimrResourceBroker::TQueueConfig &config,
               const ::NMonitoring::TDynamicCounterPtr &counters,
               TResourceLimitPtr totalLimit, TQueueCountersPtr totalCounters);

    /**
     * Return true if there are no queued tasks.
     * There still might be tasks in-fly.
     */
    bool Empty() const;

    void InsertTask(TTaskPtr task, TInstant now);
    void EraseTask(TTaskPtr task, bool finished, TInstant now);

    TTaskPtr FrontTask();
    void PopTask();

    void UpdateRealResourceUsage(TInstant now);

    // Debug print of current state into specified stream.
    void OutputState(IOutputStream &os, const TString &prefix) const;

private:
    void HoldResources(TTaskPtr task);
    void ReleaseResources(TTaskPtr task);

    void UpdatePlannedResourceUsage(TTaskPtr task, TInstant now, bool decrease);

    double GetDominantResourceComponentNormalized(const TResourceValues &values);

public:
    TString Name;
    ui32 Weight;
    TSet<TTaskPtr, TTaskEarlier> Tasks;
    TResourceLimit QueueLimit;
    TResourceLimitPtr TotalLimit;
    double RealResourceUsage;
    double PlannedResourceUsage;
    // When real resource usage was updated last time.
    TInstant UsageTimestamp;
    TQueueCounters QueueCounters;
    TQueueCountersPtr TotalCounters;
};

/**
 * Scheduler class manages tasks and queues. It directly communicates clients
 * in case of an error or successful resource allocation.
 *
 * Task submission/update/removal doesn't cause resource allocation. ScheduleTasks
 * method should be called to trigger resource allocation.
 *
 * Before next task allocation scheduler simply orders non-empty queues according
 * to their resource usage and pick the top task of the first queue. If chosen
 * task cannot be run because of total resource limit then resource allocation
 * stops. If we have resources but queue's limit doesn't allow to chosen task
 * then this queue is skipped.
 *
 * Queue's limit may be ignored in case it has no tasks running. Total limit
 * may be ignored in case there are no running tasks at all.
 *
 * When task is submitted into an empty queue we need to fix-up its resource
 * usage. It's required to avoid big differences between resource usages of
 * different queues caused by idle time of one of them. It's done by simply
 * increasing idle queue real resource usage up to minimal real resource usage
 * of non-idle queues.
 */
class TScheduler {
private:
    struct TTaskQueueLess {
        bool operator()(const TTaskQueuePtr &l, const TTaskQueuePtr &r) const;
    };

    class TTaskConfig {
    public:
        TTaskConfig(const TString &name, TDuration defaultDuration,
                    TTaskCountersPtr counters);
        TTaskConfig(const TTaskConfig &other) = default;
        TTaskConfig(TTaskConfig &&other) = default;

        TTaskConfig &operator=(const TTaskConfig &other) = default;
        TTaskConfig &operator=(TTaskConfig &&other) = default;

    public:
        TString Name;
        TTaskQueuePtr Queue;
        TDurationStat ExecTime;
        TTaskCountersPtr Counters;
    };

public:
    TScheduler(const ::NMonitoring::TDynamicCounterPtr &counters);
    ~TScheduler();

    /**
     * Returns TTask raw pointer if it exists.
     */
    const TTask* FindTask(ui64 taskId, const TActorId &client) const;
    /**
     * Create new task and queue it. Return true on success, and false if task with the same id already exists.
     */
    bool SubmitTask(const TEvResourceBroker::TTask &task, const TActorId &client, const TActorSystem &as);
    /**
     * Update queued task. Return true on success and false if task not found.
     */
    bool UpdateTask(ui64 taskId, const TActorId &client, const TResourceValues &requiredResources,
                    ui64 priority, const TString &type, bool resubmit, const TActorSystem &as);
    /**
     * Update cookie for submitted task. Return true on success and false if task not found.
     */
    bool UpdateTaskCookie(ui64 taskId, const TActorId &client, TIntrusivePtr<TThrRefBase> cookie,
                          const TActorSystem &as);

    struct TTerminateTaskResult {
        bool Success;
        TTaskPtr Task; // can be nullptr

        TTerminateTaskResult(bool success, TTaskPtr task)
            : Success(success), Task(task) {}
    };

    /**
     * Remove queued task. Return <true, task> on success, and <false, task> otherwise.
     */
    TTerminateTaskResult RemoveQueuedTask(ui64 taskId, const TActorId &client, const TActorSystem &as);
    /**
     * Finish/cancel task and release its resources. Return <true, task> on success, and <false, task> otherwise.
     */
    TTerminateTaskResult FinishTask(ui64 taskId, const TActorId &client, bool cancel, const TActorSystem &as);

    /**
     * Remove queued tasks and finish in-fly tasks submitted by specified client.
     * Return true if any task was affected.
     */
    bool RemoveTasks(const TActorId &client, const TActorSystem &as);
    /**
     * Try to allocate resources for next tasks until we are out of resources or tasks.
     */
    void ScheduleTasks(const TActorSystem &as, std::function<void(const TTask &task)> &&onTaskSchedule);

    /**
     * Build or reconfigure queues and limits according to new config. Config should
     * have at least one queue and one task type configuration (default queue and
     * unknown task). On re-configuration tasks may migrate between queues according
     * to new mapping.
     */
    void Configure(const NKikimrResourceBroker::TResourceBrokerConfig &config, const TActorSystem &as);

    /**
     * Update current scheduler time which is used for all resource consumption
     * computations.
     */
    void UpdateTime(TInstant now);

    // Debug print of current state into specified stream.
    void OutputState(IOutputStream &os) const;

private:
    // Erase task from all structure.
    void EraseTask(TTaskPtr task, bool finished, const TActorSystem &as);
    // Update resource usage for all queues.
    void UpdateResourceUsage(const TActorSystem &as);

    /**
     * Insert task to queue according to its type. Also used to attach in-fly
     * tasks to queues.
     */
    void AssignTask(TTaskPtr &task, const TActorSystem &as);
    /**
     * Get estimated execution time for task basing on default config and
     * collected statistics.
     */
    TDuration EstimateTaskExecutionTime(TTaskPtr task);

    const TTaskConfig &TaskConfig(const TString &type) const;
    TTaskConfig &TaskConfig(const TString &type);

    TInstant Now;
    THashMap<TString, TTaskQueuePtr> Queues;
    THashMap<TString, TTaskConfig> TaskConfigs;
    TResourceLimitPtr ResourceLimit;
    THashMap<std::pair<TActorId, ui64>, TTaskPtr> Tasks;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TQueueCountersPtr TotalCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr MissingTaskTypeCounter;
    ui64 NextTaskId;
};

/**
 * TResourceBroker is a simple thread-safe wrapper for scheduler.
 */
class TResourceBroker : public IResourceBroker {
public:
    TResourceBroker(const NKikimrResourceBroker::TResourceBrokerConfig &config,
                    const ::NMonitoring::TDynamicCounterPtr &counters,
                    TActorSystem *actorSystem);

    bool SubmitTaskInstant(const TEvResourceBroker::TEvSubmitTask &ev, const TActorId &sender) override;
    bool FinishTaskInstant(const TEvResourceBroker::TEvFinishTask &ev, const TActorId &sender) override;
    bool MergeTasksInstant(ui64 recipientTaskId, ui64 donorTaskId, const TActorId &sender) override;
    bool ReduceTaskResourcesInstant(ui64 taskId, const TResourceValues& reduceBy, const TActorId& sender) override;

    NKikimrResourceBroker::TResourceBrokerConfig GetConfig() const;
    void Configure(const NKikimrResourceBroker::TResourceBrokerConfig &config);

    using TOpError = THolder<TEvResourceBroker::TEvTaskOperationError>;

    TOpError SubmitTask(const TEvResourceBroker::TEvSubmitTask &ev, const TActorId &sender);
    TOpError UpdateTask(const TEvResourceBroker::TEvUpdateTask &ev, const TActorId &sender);
    TOpError UpdateTaskCookie(const TEvResourceBroker::TEvUpdateTaskCookie &ev, const TActorId &sender);
    TOpError RemoveTask(const TEvResourceBroker::TEvRemoveTask &ev, const TActorId &sender);
    TOpError FinishTask(const TEvResourceBroker::TEvFinishTask &ev, const TActorId &sender);

    void NotifyActorDied(const TEvResourceBroker::TEvNotifyActorDied &ev, const TActorId &sender);

    void OutputState(TStringStream &str);

private:
    NKikimrResourceBroker::TResourceBrokerConfig Config;
    TScheduler Scheduler;
    TMutex Lock;
    TActorSystem* ActorSystem;
};


class TResourceBrokerActor : public TActorBootstrapped<TResourceBrokerActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COMPACTION_BROKER;
    }

    TResourceBrokerActor(const NKikimrResourceBroker::TResourceBrokerConfig &config,
                         const ::NMonitoring::TDynamicCounterPtr &counters);

    void Bootstrap(const TActorContext &ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvResourceBroker::TEvSubmitTask, Handle);
            HFunc(TEvResourceBroker::TEvUpdateTask, Handle);
            HFunc(TEvResourceBroker::TEvUpdateTaskCookie, Handle);
            HFunc(TEvResourceBroker::TEvRemoveTask, Handle);
            HFunc(TEvResourceBroker::TEvFinishTask, Handle);
            HFunc(TEvResourceBroker::TEvNotifyActorDied, Handle);
            HFunc(TEvResourceBroker::TEvConfigure, Handle);
            HFunc(TEvResourceBroker::TEvConfigRequest, Handle);
            HFunc(TEvResourceBroker::TEvResourceBrokerRequest, Handle);
            HFunc(NMon::TEvHttpInfo, Handle);
        default:
            Y_ABORT("TResourceBroker::StateWork unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(),
                   ev->ToString().data());
        }
    }

private:
    void Handle(TEvResourceBroker::TEvSubmitTask::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvUpdateTask::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvUpdateTaskCookie::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvRemoveTask::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvFinishTask::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvNotifyActorDied::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvConfigure::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvResourceBroker::TEvResourceBrokerRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx);

    NKikimrResourceBroker::TResourceBrokerConfig BootstrapConfig;
    ::NMonitoring::TDynamicCounterPtr BootstrapCounters;
    TIntrusivePtr<TResourceBroker> ResourceBroker;
};

} // NResourceBroker
} // NKikimr
