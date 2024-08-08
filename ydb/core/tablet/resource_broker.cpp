#include "resource_broker_impl.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/tx/columnshard/common/limits.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NResourceBroker {

using namespace NKikimrResourceBroker;

TResourceLimit::TResourceLimit(const NKikimrResourceBroker::TResources &limit)
{
    Limit.fill(Max<ui64>());
    for (size_t i = 0; i < Limit.size() && i < limit.ResourceSize(); ++i) {
        Limit[i] = limit.GetResource(i);
    }

    if (limit.HasCpu()) {
        Limit[NKikimrResourceBroker::CPU] = limit.GetCpu();
    }

    if (limit.HasMemory()) {
        Limit[NKikimrResourceBroker::MEMORY] = limit.GetMemory();
    }

    Used.fill(0);
}

bool TResourceLimit::HasResources(const TResourceValues &values) const
{
    for (size_t i = 0; i < values.size(); ++i)
        if (values[i] + Used[i] > Limit[i])
            return false;
    return true;
}

void TResourceLimit::HoldResources(const TResourceValues &values)
{
    for (size_t i = 0; i < values.size(); ++i)
        Used[i] += values[i];
}

void TResourceLimit::ReleaseResources(const TResourceValues &values)
{
    for (size_t i = 0; i < values.size(); ++i)
        Used[i] -= values[i];
}

TTask::TTask(const TEvResourceBroker::TTask &task,
             const TActorId &client,
             TInstant timestamp,
             TTaskCountersPtr counters)
    : TEvResourceBroker::TTask(task)
    , Client(client)
    , InFly(false)
    , SubmitTime(timestamp)
    , Counters(counters)
{
    IdString = Sprintf("%s (%" PRIu64 " by %s)", Name.data(), TaskId, ToString(Client).data());
}

TString TTask::GetIdString() const
{
    return IdString;
}

void TTask::OutputState(IOutputStream &os, const TString &prefix) const
{
    os << prefix << "Task " << Name << Endl
       << prefix << "  ID: " << TaskId << Endl
       << prefix << "  Client: " << Client.ToString() << Endl
       << prefix << "  Type: " << ToString(Type) << Endl
       << prefix << "  Required resources: {" << JoinSeq(", ", RequiredResources) << "}" << Endl
       << prefix << "  Priority: " << Priority << Endl
       << prefix << "  InFly: " << InFly << Endl
       << prefix << "  Queue: " << Queue->Name << Endl
       << prefix << "  SubmitTime: " << SubmitTime.ToStringLocalUpToSeconds() << Endl;
    if (InFly)
        os << prefix << "  StartTime: " << StartTime.ToStringLocalUpToSeconds() << Endl
           << prefix << "  FinishTime: " << FinishTime.ToStringLocalUpToSeconds() << Endl;
}

TBaseCounters::TBaseCounters(const ::NMonitoring::TDynamicCounterPtr &counters)
{
    Consumption[CPU] = counters->GetCounter("CPUConsumption");
    Consumption[MEMORY] = counters->GetCounter("MemoryConsumption");
    FinishedTasks = counters->GetCounter("FinishedTasks", true);
    EnqueuedTasks = counters->GetCounter("EnqueuedTasks");
    InFlyTasks = counters->GetCounter("InFlyTasks");
}

void TBaseCounters::HoldResources(const TResourceValues &values)
{
    for (size_t i = 0; i < Min(values.size(), Consumption.size()); ++i)
        *Consumption[i] += values[i];
}

void TBaseCounters::ReleaseResources(const TResourceValues &values)
{
    for (size_t i = 0; i < Min(values.size(), Consumption.size()); ++i)
        *Consumption[i] -= values[i];
}

TQueueCounters::TQueueCounters(const ::NMonitoring::TDynamicCounterPtr &counters)
    : TBaseCounters(counters)
{
}

TTaskCounters::TTaskCounters(const ::NMonitoring::TDynamicCounterPtr &counters)
    : TBaseCounters(counters)
{
}

TDurationStat::TDurationStat(TDuration def,
                             size_t history)
    : Current(0)
{
    Y_ABORT_UNLESS(history > 0);
    Values.resize(history, def);
    Total = def * history;
}

void TDurationStat::Add(TDuration duration)
{
    Total -= Values[Current];
    Values[Current] = duration;
    Total += duration;

    ++Current;
    if (Current == Values.size())
        Current = 0;
}

TDuration TDurationStat::GetAverage() const
{
    return Total / Max<size_t>(1, Values.size());
}

bool TTaskQueue::TTaskEarlier::operator()(const TTaskPtr &l,
                                          const TTaskPtr &r) const
{
    if (l->Priority != r->Priority)
        return l->Priority < r->Priority;

    if (l->SubmitTime != r->SubmitTime)
        return l->SubmitTime < r->SubmitTime;

    if (l->TaskId != r->TaskId)
        return l->TaskId < r->TaskId;

    return l->Client < r->Client;
}

TTaskQueue::TTaskQueue(const NKikimrResourceBroker::TQueueConfig &config,
                       const ::NMonitoring::TDynamicCounterPtr &counters,
                       TResourceLimitPtr totalLimit,
                       TQueueCountersPtr totalCounters)
    : Name(config.GetName())
    , Weight(config.GetWeight())
    , QueueLimit(config.GetLimit())
    , TotalLimit(totalLimit)
    , RealResourceUsage(0)
    , PlannedResourceUsage(0)
    , UsageTimestamp(TInstant::MicroSeconds(0))
    , QueueCounters(counters->GetSubgroup("queue", Name))
    , TotalCounters(totalCounters)
{
}

bool TTaskQueue::Empty() const
{
    return Tasks.empty();
}

void TTaskQueue::InsertTask(TTaskPtr task, TInstant now)
{
    Y_ABORT_UNLESS(!task->Queue, "TTaskQueue::InsertTask: task is already in resource queue");

    if (task->InFly) {
        // Update resource consumption.
        HoldResources(task);
        UpdatePlannedResourceUsage(task, now, false);
        UpdateRealResourceUsage(now);

        // Update queue/task counters
        QueueCounters.InFlyTasks->Inc();
        TotalCounters->InFlyTasks->Inc();
        task->Counters->InFlyTasks->Inc();
    } else {
        Tasks.insert(task);

        // Update queue/task counters
        QueueCounters.EnqueuedTasks->Inc();
        TotalCounters->EnqueuedTasks->Inc();
        task->Counters->EnqueuedTasks->Inc();
    }

    task->Queue = this;
}

void TTaskQueue::EraseTask(TTaskPtr task, bool finished, TInstant now)
{
    Y_ABORT_UNLESS(task->Queue.Get() == this);
    Y_ABORT_UNLESS(task->InFly || !finished);

    if (task->InFly) {
        // Update resources consumption.
        UpdateRealResourceUsage(now);
        UpdatePlannedResourceUsage(task, now, true);
        ReleaseResources(task);

        // Update queue/task counters
        QueueCounters.InFlyTasks->Dec();
        TotalCounters->InFlyTasks->Dec();
        task->Counters->InFlyTasks->Dec();

        if (finished) {
            QueueCounters.FinishedTasks->Inc();
            TotalCounters->FinishedTasks->Inc();
            task->Counters->FinishedTasks->Inc();
        }
    } else {
        // Removing the first element is the most common case.
        if (*Tasks.begin() == task) {
            Tasks.erase(Tasks.begin());
        } else {
            Tasks.erase(task);
        }

        // Update queue/task counters
        QueueCounters.EnqueuedTasks->Dec();
        TotalCounters->EnqueuedTasks->Dec();
        task->Counters->EnqueuedTasks->Dec();
    }
    task->Queue = nullptr;
}

TTaskPtr TTaskQueue::FrontTask()
{
    Y_ABORT_UNLESS(!Tasks.empty());
    return *Tasks.begin();
}

void TTaskQueue::PopTask()
{
    Y_ABORT_UNLESS(!Tasks.empty());
    EraseTask(*Tasks.begin(), false, TInstant());
}

void TTaskQueue::HoldResources(TTaskPtr task)
{
    const TResourceValues &values = task->RequiredResources;

    QueueLimit.HoldResources(values);
    TotalLimit->HoldResources(values);

    QueueCounters.HoldResources(values);
    TotalCounters->HoldResources(values);
    task->Counters->HoldResources(values);
}

void TTaskQueue::ReleaseResources(TTaskPtr task)
{
    const TResourceValues &values = task->RequiredResources;

    QueueLimit.ReleaseResources(values);
    TotalLimit->ReleaseResources(values);

    QueueCounters.ReleaseResources(values);
    TotalCounters->ReleaseResources(values);
    task->Counters->ReleaseResources(values);
}

void TTaskQueue::UpdateRealResourceUsage(TInstant now)
{
    auto duration = now - UsageTimestamp;
    if (!duration)
        return;

    // Find dominant resource consumption and update usage
    auto dom = GetDominantResourceComponentNormalized(QueueLimit.Used);
    auto usage = RealResourceUsage + dom * duration.MilliSeconds() / Max(1u, Weight);
    RealResourceUsage = usage;

    UsageTimestamp = now;
}

void TTaskQueue::UpdatePlannedResourceUsage(TTaskPtr task,
                                            TInstant now,
                                            bool decrease)
{
    auto duration = task->FinishTime - now;
    if (!duration)
        return;

    if (decrease)
        // Round up to ms when decreasing to match real and planned usage.
        // Otherwise 1ms goes to planned and doesn't go to real usage.
        duration += TDuration::MicroSeconds(999);

    auto dom = GetDominantResourceComponentNormalized(task->RequiredResources);
    if (decrease) {
        PlannedResourceUsage -= dom * duration.MilliSeconds() / Max(1u, Weight);
        PlannedResourceUsage = Max(PlannedResourceUsage, RealResourceUsage);
    } else {
        PlannedResourceUsage = Max(PlannedResourceUsage, RealResourceUsage);
        PlannedResourceUsage += dom * duration.MilliSeconds() / Max(1u, Weight);
    }
}

double TTaskQueue::GetDominantResourceComponentNormalized(const TResourceValues &values)
{
    std::array<double, RESOURCE_COUNT> norm;
    for (size_t i = 0; i < norm.size(); ++i)
        norm[i] = (double)QueueLimit.Used[i] / (double)Max(1lu, TotalLimit->Limit[i]);
    size_t i = MaxElement(norm.begin(), norm.end()) - norm.begin();
    return (double)values[i] / (double)Max(1lu, TotalLimit->Limit[i]);
}

void TTaskQueue::OutputState(IOutputStream &os, const TString &prefix) const
{
    os << prefix << "Queue " << Name << Endl
       << prefix << "  Weight: " << Weight << Endl
       << prefix << "  QueueLimit: {" << JoinSeq(", ", QueueLimit.Limit) << "}" << Endl
       << prefix << "  QueueConsumption: {" << JoinSeq(", ", QueueLimit.Used) << "}" << Endl
       << prefix << "  Real resource usage: " << RealResourceUsage << " (updated "
       << UsageTimestamp.ToStringLocalUpToSeconds() << ")" << Endl
       << prefix << "  Planned resource usage: " << PlannedResourceUsage << Endl
       << prefix << "  Enqueued tasks:" << Endl;

    for (auto it = Tasks.begin(); it != Tasks.end(); ++it)
        (*it)->OutputState(os, prefix + "    ");
}

bool TScheduler::TTaskQueueLess::operator()(const TTaskQueuePtr &l,
                                            const TTaskQueuePtr &r) const
{
    auto lu = Max(l->RealResourceUsage, l->PlannedResourceUsage);
    auto ru = Max(r->RealResourceUsage, r->PlannedResourceUsage);

    if (lu != ru)
        return lu < ru;

    if (l->Weight != r->Weight)
        return l->Weight > r->Weight;

    return l.Get() < r.Get();
};

TScheduler::TTaskConfig::TTaskConfig(const TString &name,
                                     TDuration defaultDuration,
                                     TTaskCountersPtr counters)
    : Name(name)
    , ExecTime(defaultDuration, 20)
    , Counters(counters)
{
}

TScheduler::TScheduler(const ::NMonitoring::TDynamicCounterPtr &counters)
    : Counters(counters)
    , TotalCounters(new TQueueCounters(counters->GetSubgroup("queue", "total")))
    , MissingTaskTypeCounter(counters->GetCounter("MissingTaskType", true))
    , NextTaskId(1)
{
}

TScheduler::~TScheduler()
{
    for (auto &entry : Tasks)
        entry.second->Queue->EraseTask(entry.second, false, Now);
}

const TTask* TScheduler::FindTask(ui64 taskId, const TActorId &client) const {
    auto it = Tasks.find(std::make_pair(client, taskId));
    return it != Tasks.end() ? it->second.Get() : nullptr;
}

bool TScheduler::SubmitTask(const TEvResourceBroker::TTask &task,
                            const TActorId &client,
                            const TActorSystem &as)
{
    auto &config = TaskConfig(task.Type);
    TTaskPtr newTask = new TTask(task, client, Now, config.Counters);

    LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
              "Submitted new %s task %s priority=%" PRIu64 " resources={%s}",
              config.Name.data(), newTask->GetIdString().data(), task.Priority,
              JoinSeq(", ", task.RequiredResources).data());

    auto id = std::make_pair(client, task.TaskId);
    if (!task.TaskId) {
        do {
            id = std::make_pair(client, NextTaskId++);
        } while (Tasks.contains(id));
        newTask->TaskId = id.second;

        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Use ID %" PRIu64 " for submitted task",
                  id.second);
    } else if (Tasks.contains(id)) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
            "SubmitTask failed for task %" PRIu64 " to %s: task with the same ID has been already submitted",
            task.TaskId, ToString(client).c_str());
        return false;
    }

    if (!TaskConfigs.contains(newTask->Type))
        MissingTaskTypeCounter->Inc();

    Tasks.emplace(id, newTask);
    AssignTask(newTask, as);

    return true;
}

bool TScheduler::UpdateTask(ui64 taskId,
                            const TActorId &client,
                            const TResourceValues &requiredResources,
                            ui64 priority,
                            const TString &type,
                            bool resubmit,
                            const TActorSystem &as)
{
    auto it = Tasks.find(std::make_pair(client, taskId));
    if (it == Tasks.end()) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "UpdateTask failed for task %" PRIu64 " to %s: cannot update unknown task",
                  taskId, ToString(client).c_str());
        return false;
    }

    auto task = it->second;
    LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
              "Update task %s (priority=%" PRIu64 " type=%s resources={%s} resubmit=%" PRIu32 ")",
              task->GetIdString().data(), priority, type.data(), JoinSeq(", ", requiredResources).data(),
              (ui32)resubmit);

    auto queue = task->Queue;
    queue->EraseTask(task, false, Now);

    task->RequiredResources = requiredResources;
    task->Priority = priority;
    task->Type = type;
    if (resubmit && task->InFly)
        task->InFly = false;

    AssignTask(task, as);

    return true;
}

bool TScheduler::UpdateTaskCookie(ui64 taskId,
                                  const TActorId &client,
                                  TIntrusivePtr<TThrRefBase> cookie,
                                  const TActorSystem &as)
{
    auto it = Tasks.find(std::make_pair(client, taskId));
    if (it == Tasks.end()) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "UpdateTaskCookie failed for task %" PRIu64 " to %s: cannot update unknown task's cookie",
                  taskId, ToString(client).c_str());
        return false;
    }

    auto task = it->second;
    LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
              "Update cookie for task %s", task->GetIdString().data());

    task->Cookie = cookie;
    return true;
}

TScheduler::TTerminateTaskResult TScheduler::RemoveQueuedTask(ui64 taskId,
                                                              const TActorId &client,
                                                              const TActorSystem &as)
{
    auto it = Tasks.find(std::make_pair(client, taskId));
    if (it == Tasks.end()) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "RemoveQueuedTask failed for task %" PRIu64 " to %s: cannot remove unknown task",
                  taskId, ToString(client).c_str());
        return TTerminateTaskResult(false, nullptr);
    }

    auto task = it->second;
    if (task->InFly) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "RemoveQueuedTask failed for task %" PRIu64 " to %s: cannot remove in-fly task",
                  taskId, ToString(client).c_str());
        return TTerminateTaskResult(false, task);
    }

    LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER, "Removing task %s",
              task->GetIdString().data());

    EraseTask(task, false, as);

    return TTerminateTaskResult(true, task);
}

TScheduler::TTerminateTaskResult TScheduler::FinishTask(ui64 taskId,
                                                        const TActorId &client,
                                                        bool cancel,
                                                        const TActorSystem &as)
{
    auto it = Tasks.find(std::make_pair(client, taskId));
    if (it == Tasks.end()) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "FinishTask failed for task %" PRIu64 " to %s: cannot finish unknown task",
                  taskId, ToString(client).c_str());
        return TTerminateTaskResult(false, nullptr);
    }

    auto task = it->second;
    if (!task->InFly) {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "FinishTask failed for task %" PRIu64 " to %s: cannot finish queued task",
                  taskId, ToString(client).c_str());
        return TTerminateTaskResult(false, task);
    }

    LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER, "Finish task %s (release resources {%s})",
              task->GetIdString().data(), JoinSeq(", ", task->RequiredResources).data());

    // Add execution time to statistics but cancelled tasks
    // don't affect average execution time.
    auto &config = TaskConfig(task->Type);
    auto execTime = Now - task->StartTime;
    if (!cancel)
        config.ExecTime.Add(execTime);

    EraseTask(task, true, as);

    return TTerminateTaskResult(true, task);
}

bool TScheduler::RemoveTasks(const TActorId &client, const TActorSystem &as)
{
    bool res = false;

    for (auto it = Tasks.begin(); it != Tasks.end(); ) {
        if (it->first.first == client) {
            auto id = it->first.second;
            bool inFly = it->second->InFly;

            ++it;

            if (inFly)
                FinishTask(id, client, true, as);
            else
                RemoveQueuedTask(id, client, as);

            res = true;
        } else
            ++it;
    }

    return res;
}

void TScheduler::EraseTask(TTaskPtr task, bool finished, const TActorSystem &as)
{
    auto queue = task->Queue;
    auto oldp = queue->PlannedResourceUsage;
    auto oldr = queue->RealResourceUsage;

    queue->EraseTask(task, finished, Now);

    if (oldp != queue->PlannedResourceUsage)
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Updated planned resource usage for queue %s from %f to %f (remove task %s)",
                  queue->Name.data(), oldp, queue->PlannedResourceUsage, task->GetIdString().data());

    if (oldr != queue->RealResourceUsage)
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Updated real resource usage for queue %s from %f to %f",
                  queue->Name.data(), oldr, queue->RealResourceUsage);

    Tasks.erase(std::make_pair(task->Client, task->TaskId));
}

void TScheduler::ScheduleTasks(const TActorSystem &as,
                               std::function<void(const TTask &task)> &&onTaskSchedule)
{
    UpdateResourceUsage(as);

    TSet<TTaskQueuePtr, TTaskQueueLess> pending;
    for (auto &entry : Queues) {
        auto &queue = entry.second;
        if (!queue->Empty())
            pending.insert(queue);
    }

    ui64 blockedResources = 0;
    while (!pending.empty()) {
        auto queue = *pending.begin();
        pending.erase(pending.begin());

        auto task = queue->FrontTask();
        if (task->GetRequiredResourcesMask() & blockedResources) {
            LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                      "Skip queue %s blocked by an earlier queue",
                      queue->Name.c_str());
            continue;
        }

        // If task is out of total limits then we have to wait until
        // resources are released.
        // Allow resource over-usage if no tasks are running.
        if (!ResourceLimit->HasResources(task->RequiredResources)
            && *TotalCounters->InFlyTasks) {
            LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                      "Not enough resources to start task %s",
                      task->GetIdString().data());
            blockedResources |= task->GetRequiredResourcesMask();
            continue;
        }

        // If task is out of queue limits then skip it.
        // Allow resource over-usage if no tasks are running in this queue.
        if (!queue->QueueLimit.HasResources(task->RequiredResources)
            && *queue->QueueCounters.InFlyTasks) {
            LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                      "Skip queue %s due to exceeded limits",
                      queue->Name.data());
            continue;
        }

        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Allocate resources {%s} for task %s from queue %s",
                  JoinSeq(", ", task->RequiredResources).data(),
                  task->GetIdString().data(), queue->Name.data());

        queue->PopTask();
        task->InFly = true;
        task->StartTime = Now;
        task->FinishTime = Now + EstimateTaskExecutionTime(task);
        AssignTask(task, as);

        onTaskSchedule(*task);

        if (!queue->Empty())
            pending.insert(queue);
    }
}

void TScheduler::UpdateResourceUsage(const TActorSystem &as)
{
    for (auto &entry : Queues) {
        auto &queue = entry.second;
        auto old = queue->RealResourceUsage;
        queue->UpdateRealResourceUsage(Now);

        if (old != queue->RealResourceUsage)
            LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                      "Updated real resource usage for queue %s from %f to %f (in-fly consumption {%s})",
                      queue->Name.data(), old, queue->RealResourceUsage,
                      JoinSeq(", ", queue->QueueLimit.Used).data());
    }
}

void TScheduler::AssignTask(TTaskPtr &task, const TActorSystem &as)
{
    TString state = task->InFly ? "in-fly" : "waiting";
    TTaskQueuePtr queue = TaskConfig(task->Type).Queue;

    if (!TaskConfigs.contains(task->Type)) {
        LOG_ERROR(as, NKikimrServices::RESOURCE_BROKER,
                  "Assigning %s task '%s' of unknown type '%s' to default queue",
                  state.data(), task->GetIdString().data(), task->Type.data());
    } else {
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Assigning %s task %s to queue %s",
                  state.data(), task->GetIdString().data(), queue->Name.data());
    }

    auto oldp = queue->PlannedResourceUsage;
    auto oldr = queue->RealResourceUsage;
    double usage = Max<double>();
    bool forceUsage = false;

    // If queue is empty then we might need to update its resource
    // usage. Otherwise unused for a long time queue may get too
    // much priority.
    if (!task->InFly && queue->Empty()) {
        for (auto &entry : Queues) {
            if (!entry.second->Empty()) {
                usage = Min(usage, entry.second->RealResourceUsage);
                forceUsage = true;
            }
        }
    }

    if (forceUsage)
        queue->RealResourceUsage = Max(queue->RealResourceUsage, usage);
    queue->InsertTask(task, Now);

    if (oldr != queue->RealResourceUsage)
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Updated real resource usage for queue %s from %f to %f",
                  queue->Name.data(), oldr, queue->RealResourceUsage);

    if (oldp != queue->PlannedResourceUsage)
        LOG_DEBUG(as, NKikimrServices::RESOURCE_BROKER,
                  "Updated planned resource usage for queue %s from %f to %f (insert task %s)",
                  queue->Name.data(), oldp, queue->PlannedResourceUsage, task->GetIdString().data());
}

const TScheduler::TTaskConfig &TScheduler::TaskConfig(const TString &type) const
{
    if (TaskConfigs.contains(type))
        return TaskConfigs.at(type);
    return TaskConfigs.at(NLocalDb::UnknownTaskName);
}

TScheduler::TTaskConfig &TScheduler::TaskConfig(const TString &type)
{
    if (TaskConfigs.contains(type))
        return TaskConfigs.at(type);
    return TaskConfigs.at(NLocalDb::UnknownTaskName);
}

TDuration TScheduler::EstimateTaskExecutionTime(TTaskPtr task)
{
    return TaskConfig(task->Type).ExecTime.GetAverage();
}

void TScheduler::Configure(const TResourceBrokerConfig &config, const TActorSystem &as)
{
    // Remove all tasks from queues.
    for (auto &entry : Tasks)
        entry.second->Queue->EraseTask(entry.second, false, Now);
    // Remove old queues.
    Queues.clear();

    // Replace limit with new one.
    ResourceLimit = new TResourceLimit(config.GetResourceLimit());

    // Create new queues.
    for (const auto &queueConfig : config.GetQueues()) {
        TTaskQueuePtr queue = new TTaskQueue(queueConfig, Counters,
                                             ResourceLimit, TotalCounters);
        Queues.emplace(queue->Name, queue);
    }
    Y_ABORT_UNLESS(Queues.contains(NLocalDb::DefaultQueueName), "default queue '%s' wasn't found in config", NLocalDb::DefaultQueueName.data());

    // Read new tasks config.
    TaskConfigs.clear();
    for (auto &task : config.GetTasks()) {
        TTaskCountersPtr counters = new TTaskCounters(Counters->GetSubgroup("task", task.GetName()));
        TTaskConfig taskConfig(task.GetName(),
                               TDuration::MicroSeconds(task.GetDefaultDuration()),
                               counters);
        Y_ABORT_UNLESS(Queues.contains(task.GetQueueName()), " queue '%s' wasn't found in config", task.GetQueueName().data());
        taskConfig.Queue = Queues.at(task.GetQueueName());

        TaskConfigs.emplace(taskConfig.Name, taskConfig);
    }
    Y_ABORT_UNLESS(TaskConfigs.contains(NLocalDb::UnknownTaskName), "task '%s' wasn't found in config", NLocalDb::UnknownTaskName.data());

    // Move all tasks to queues.
    for (auto &entry : Tasks)
        AssignTask(entry.second, as);
}

void TScheduler::UpdateTime(TInstant now)
{
    Now = now;
}

void TScheduler::OutputState(IOutputStream &os) const
{
    os << "Resource broker counters:" << Endl;
    for (auto &entry : Queues) {
        os << "  queue=" << entry.first << ":" << Endl;
        Counters->GetSubgroup("queue", entry.first)->OutputPlainText(os, "    ");
    }
    os << "  queue=total:" << Endl;
    Counters->GetSubgroup("queue", "total")->OutputPlainText(os, "    ");
    for (auto &entry : TaskConfigs) {
        os << "  task=" << entry.first << ":" << Endl;
        Counters->GetSubgroup("task", entry.first)->OutputPlainText(os, "    ");
    }
    os << "  MissingTaskType: " << Counters->GetCounter("MissingTaskType", true)->Val() << Endl;
    os << "Scheduler resource limit: {" << JoinSeq(", ", ResourceLimit->Limit) << "}" << Endl
       << "Total resource consumption: {" << JoinSeq(", ", ResourceLimit->Used) << "}" << Endl;

    os << Endl << "Queues state:" << Endl;
    for (auto &entry : Queues)
        entry.second->OutputState(os, "  ");

    os << Endl << "In-Fly tasks:" << Endl;
    for (auto &entry : Tasks)
        if (entry.second->InFly)
            entry.second->OutputState(os, "  ");
}

TResourceBroker::TResourceBroker(const TResourceBrokerConfig &config,
                                 const ::NMonitoring::TDynamicCounterPtr &counters,
                                 TActorSystem *actorSystem)
    : Config(config)
    , Scheduler(counters)
    , ActorSystem(actorSystem)
{
    with_lock(Lock) {
        Scheduler.Configure(Config, *ActorSystem);
    }
}

void TResourceBroker::Configure(const TResourceBrokerConfig &config)
{
    with_lock(Lock) {
        Config = config;

        Scheduler.UpdateTime(ActorSystem->Timestamp());
        Scheduler.Configure(Config, *ActorSystem);
        Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
            ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
        });
    }
}

TResourceBroker::TOpError TResourceBroker::SubmitTask(const TEvResourceBroker::TEvSubmitTask &ev,
                                                      const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());
        if (Scheduler.SubmitTask(ev.Task, sender, *ActorSystem)) {
            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });
            return {};
        }
    }

    auto error = MakeHolder<TEvResourceBroker::TEvTaskOperationError>();
    error->TaskId = ev.Task.TaskId;
    error->Status.Code = TEvResourceBroker::TStatus::ALREADY_EXISTS;
    error->Status.Message = "task with the same ID has been already submitted";
    error->Cookie = ev.Task.Cookie;
    return error;
}

bool TResourceBroker::SubmitTaskInstant(const TEvResourceBroker::TEvSubmitTask& ev, const TActorId& sender)
{
    with_lock(Lock) {
        bool success = false;

        Scheduler.UpdateTime(ActorSystem->Timestamp());
        if (Scheduler.SubmitTask(ev.Task, sender, *ActorSystem)) {
            Scheduler.ScheduleTasks(*ActorSystem, [this, taskId = ev.Task.TaskId, &success](const TTask &task) {
                if (task.TaskId == taskId) {
                    success = true;
                } else {
                    ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
                }
            });

            if (!success) {
                auto removed = Scheduler.RemoveQueuedTask(ev.Task.TaskId, sender, *ActorSystem);
                Y_DEBUG_ABORT_UNLESS(removed.Success);
            }
        }

        return success;
    }
}

TResourceBroker::TOpError TResourceBroker::UpdateTask(const TEvResourceBroker::TEvUpdateTask& ev,
                                                      const TActorId& sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());
        if (Scheduler.UpdateTask(ev.TaskId, sender, ev.RequiredResources, ev.Priority, ev.Type, ev.Resubmit, *ActorSystem)) {
            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });
            return {};
        }
    }

    auto error = MakeHolder<TEvResourceBroker::TEvTaskOperationError>();
    error->TaskId = ev.TaskId;
    error->Status.Code = TEvResourceBroker::TStatus::UNKNOWN_TASK;
    error->Status.Message = "cannot update unknown task";
    error->Cookie = nullptr;
    return error;
}

bool TResourceBroker::MergeTasksInstant(ui64 recipientTaskId, ui64 donorTaskId, const TActorId &sender) {
    with_lock(Lock) {
        auto recipientTask = Scheduler.FindTask(recipientTaskId, sender);
        if (!recipientTask || !recipientTask->InFly) {
            return false;
        }

        auto donorTask = Scheduler.FindTask(donorTaskId, sender);
        if (!donorTask || !donorTask->InFly) {
            return false;
        }

        if (recipientTask->Type != donorTask->Type) {
            return false;
        }

        TResourceValues mergedResources;
        for (ui64 i = 0; i < recipientTask->RequiredResources.size(); ++i) {
            mergedResources[i] = recipientTask->RequiredResources[i] + donorTask->RequiredResources[i];
        }

        Scheduler.UpdateTime(ActorSystem->Timestamp());

        bool updated = Scheduler.UpdateTask(recipientTaskId, sender, mergedResources, recipientTask->Priority,
            recipientTask->Type, /* resubmit */ false, *ActorSystem);
        Y_DEBUG_ABORT_UNLESS(updated);

        auto finished = Scheduler.FinishTask(donorTaskId, sender, /* cancel */ false, *ActorSystem);
        Y_DEBUG_ABORT_UNLESS(finished.Success);

        Scheduler.ScheduleTasks(*ActorSystem, [this, recipientTaskId](const TTask &task) {
            Y_DEBUG_ABORT_UNLESS(task.TaskId != recipientTaskId);
            ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
        });

        return true;
    }
}

bool TResourceBroker::ReduceTaskResourcesInstant(ui64 taskId, const TResourceValues& reduceBy, const TActorId& sender)
{
    with_lock(Lock) {
        auto task = Scheduler.FindTask(taskId, sender);
        if (!task) {
            return false;
        }

        auto resources = task->RequiredResources;
        for (ui32 i = 0; i < resources.size(); ++i) {
            if (i < reduceBy.size()) {
                resources[i] = resources[i] > reduceBy[i] ? resources[i] - reduceBy[i] : 0;
            }
        }

        Scheduler.UpdateTime(ActorSystem->Timestamp());

        if (Scheduler.UpdateTask(taskId, sender, resources, task->Priority, task->Type, false, *ActorSystem)) {
            Scheduler.ScheduleTasks(*ActorSystem, [this, taskId](const TTask &task) {
                if (task.TaskId != taskId) {
                    ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
                }
            });
            return true;
        }

        LOG_ERROR(*ActorSystem, NKikimrServices::RESOURCE_BROKER,
            "ReduceTaskResourcesInstant failed for task %" PRIu64, taskId);
        return false;
    }
}

TResourceBroker::TOpError TResourceBroker::UpdateTaskCookie(const TEvResourceBroker::TEvUpdateTaskCookie &ev,
                                                            const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());
        if (Scheduler.UpdateTaskCookie(ev.TaskId, sender, ev.Cookie, *ActorSystem)) {
            return {};
        }
    }

    auto error = MakeHolder<TEvResourceBroker::TEvTaskOperationError>();
    error->TaskId = ev.TaskId;
    error->Status.Code = TEvResourceBroker::TStatus::UNKNOWN_TASK;
    error->Status.Message = "cannot update unknown task's cookie";
    error->Cookie = nullptr;
    return error;
}

TResourceBroker::TOpError TResourceBroker::RemoveTask(const TEvResourceBroker::TEvRemoveTask &ev,
                                                      const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());

        auto result = Scheduler.RemoveQueuedTask(ev.TaskId, sender, *ActorSystem);

        if (result.Success) {
            if (ev.ReplyOnSuccess) {
                auto resp = MakeHolder<TEvResourceBroker::TEvTaskRemoved>();
                resp->TaskId = result.Task->TaskId;
                resp->Cookie = result.Task->Cookie;

                ActorSystem->Send(sender, resp.Release());
            }

            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });

            return {};
        }

        auto error = MakeHolder<TEvResourceBroker::TEvTaskOperationError>();
        error->TaskId = ev.TaskId;

        if (result.Task) {
            error->Status.Code = TEvResourceBroker::TStatus::TASK_IN_FLY;
            error->Status.Message = "cannot remove in-fly task";
            error->Cookie = result.Task->Cookie;
        } else {
            error->Status.Code = TEvResourceBroker::TStatus::UNKNOWN_TASK;
            error->Status.Message = "cannot remove unknown task";
            error->Cookie = nullptr;
        }

        return error;
    }
}

TResourceBroker::TOpError TResourceBroker::FinishTask(const TEvResourceBroker::TEvFinishTask &ev,
                                                      const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());

        auto result = Scheduler.FinishTask(ev.TaskId, sender, ev.Cancel, *ActorSystem);

        if (result.Success) {
            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });

            return {};
        }

        auto error = MakeHolder<TEvResourceBroker::TEvTaskOperationError>();
        error->TaskId = ev.TaskId;

        if (result.Task) {
            error->Status.Code = TEvResourceBroker::TStatus::TASK_IN_QUEUE;
            error->Status.Message = "cannot finish queued task";
            error->Cookie = result.Task->Cookie;
        } else {
            error->Status.Code = TEvResourceBroker::TStatus::UNKNOWN_TASK;
            error->Status.Message = "cannot finish unknown task";
            error->Cookie = nullptr;
        }

        return error;
    }
}

bool TResourceBroker::FinishTaskInstant(const TEvResourceBroker::TEvFinishTask &ev,
                                        const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());

        auto result = Scheduler.FinishTask(ev.TaskId, sender, ev.Cancel, *ActorSystem);

        if (result.Success) {
            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });
        } else {
            LOG_ERROR(*ActorSystem, NKikimrServices::RESOURCE_BROKER,
                "FinishTaskInstant failed for task %" PRIu64 ": %s",
                ev.TaskId, (result.Task ? "cannot finish queued task" : "cannot finish unknown task"));
        }

        return result.Success;
    }
}

void TResourceBroker::NotifyActorDied(const TEvResourceBroker::TEvNotifyActorDied &, const TActorId &sender)
{
    with_lock(Lock) {
        Scheduler.UpdateTime(ActorSystem->Timestamp());
        if (Scheduler.RemoveTasks(sender, *ActorSystem)) {
            Scheduler.ScheduleTasks(*ActorSystem, [this](const TTask &task) {
                ActorSystem->Send(task.Client, new TEvResourceBroker::TEvResourceAllocated(task.TaskId, task.Cookie));
            });
        }
    }
}

void TResourceBroker::OutputState(TStringStream& str)
{
    with_lock(Lock) {
        Scheduler.OutputState(str);
    }
}

TResourceBrokerActor::TResourceBrokerActor(const TResourceBrokerConfig &config,
                                           const ::NMonitoring::TDynamicCounterPtr &counters)
    : Config(config)
    , Counters(counters)
{
}

void TResourceBrokerActor::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::RESOURCE_BROKER, "TResourceBrokerActor bootstrap");

    NActors::TMon* mon = AppData(ctx)->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "rb", "Resource broker",
                               false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
    }

    ResourceBroker = MakeIntrusive<TResourceBroker>(std::move(Config), std::move(Counters), ctx.ActorSystem());
    Become(&TThis::StateWork);
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvSubmitTask::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto error = ResourceBroker->SubmitTask(*ev->Get(), ev->Sender);

    if (error) {
        ctx.Send(ev->Sender, error.Release());
    }
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvUpdateTask::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto error = ResourceBroker->UpdateTask(*ev->Get(), ev->Sender);

    if (error) {
        ctx.Send(ev->Sender, error.Release());
    }
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvUpdateTaskCookie::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto error = ResourceBroker->UpdateTaskCookie(*ev->Get(), ev->Sender);

    if (error) {
        ctx.Send(ev->Sender, error.Release());
    }
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvRemoveTask::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto error = ResourceBroker->RemoveTask(*ev->Get(), ev->Sender);

    if (error) {
        ctx.Send(ev->Sender, error.Release());
    }
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvFinishTask::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto error = ResourceBroker->FinishTask(*ev->Get(), ev->Sender);

    if (error) {
        ctx.Send(ev->Sender, error.Release());
    }
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvNotifyActorDied::TPtr &ev,
                                  const TActorContext &)
{
    ResourceBroker->NotifyActorDied(*ev->Get(), ev->Sender);
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvConfigure::TPtr &ev,
                                  const TActorContext &ctx)
{
    auto &config = ev->Get()->Record;
    if (ev->Get()->Merge) {
        LOG_INFO_S(ctx, NKikimrServices::RESOURCE_BROKER, "New config diff: " << config.ShortDebugString());
        auto copy = Config;
        MergeConfigUpdates(copy, config);
        config.Swap(&copy);
    }

    LOG_INFO_S(ctx, NKikimrServices::RESOURCE_BROKER, "New config: " << config.ShortDebugString());

    TSet<TString> queues;
    TSet<TString> tasks;
    bool success = true;
    TString error;
    for (auto &queue : config.GetQueues())
        queues.insert(queue.GetName());
    for (auto &task : config.GetTasks()) {
        if (!queues.contains(task.GetQueueName())) {
            error = Sprintf("task '%s' uses unknown queue '%s'", task.GetName().data(), task.GetQueueName().data());
            success = false;
            break;
        }
        tasks.insert(task.GetName());
    }
    if (success && !queues.contains(NLocalDb::DefaultQueueName)) {
        error = Sprintf("queue '%s' is required", NLocalDb::DefaultQueueName.c_str());
        success = false;
    }
    if (success && !tasks.contains(NLocalDb::UnknownTaskName)) {
        error = Sprintf("task '%s' is required", NLocalDb::UnknownTaskName.c_str());
        success = false;
    }

    TAutoPtr<TEvResourceBroker::TEvConfigureResult> response = new TEvResourceBroker::TEvConfigureResult;

    if (!success) {
        response->Record.SetSuccess(false);
        response->Record.SetMessage(error);
    } else if (!queues.contains(NLocalDb::DefaultQueueName)) {
        response->Record.SetSuccess(false);
        response->Record.SetMessage("no default queue in config");
    } else if (!tasks.contains(NLocalDb::UnknownTaskName)) {
        response->Record.SetSuccess(false);
        response->Record.SetMessage("no unknown task in config");
    } else {
        response->Record.SetSuccess(true);

        ResourceBroker->Configure(std::move(ev->Get()->Record));
    }

    LOG_INFO_S(ctx, NKikimrServices::RESOURCE_BROKER, "Configure result: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release());
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvConfigRequest::TPtr& ev, const TActorContext&)
{
    auto resp = MakeHolder<TEvResourceBroker::TEvConfigResponse>();
    for (auto& queue : Config.GetQueues()) {
        if (queue.GetName() == ev->Get()->Queue) {
            resp->QueueConfig = queue;
            break;
        }
    }
    Send(ev->Sender, resp.Release());
}

void TResourceBrokerActor::Handle(TEvResourceBroker::TEvResourceBrokerRequest::TPtr &ev, const TActorContext &ctx)
{
    auto resp = MakeHolder<TEvResourceBroker::TEvResourceBrokerResponse>();
    resp->ResourceBroker = ResourceBroker;

    ctx.Send(ev->Sender, resp.Release());
}

void TResourceBrokerActor::Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx)
{
    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Current config:" << Endl
                << Config.DebugString() << Endl;
            ResourceBroker->OutputState(str);
        }
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

NKikimrResourceBroker::TResourceBrokerConfig MakeDefaultConfig()
{
    NKikimrResourceBroker::TResourceBrokerConfig config;

    const ui64 DefaultQueueCPU = 2;

    const ui64 KqpRmQueueCPU = 4;
    const ui64 KqpRmQueueMemory = 10ULL << 30;

    const ui64 CSTTLCompactionMemoryLimit = NOlap::TGlobalLimits::TTLCompactionMemoryLimit;
    const ui64 CSInsertCompactionMemoryLimit = NOlap::TGlobalLimits::InsertCompactionMemoryLimit;
    const ui64 CSGeneralCompactionMemoryLimit = NOlap::TGlobalLimits::GeneralCompactionMemoryLimit;
    const ui64 CSScanMemoryLimit = NOlap::TGlobalLimits::ScanMemoryLimit;

    const ui64 TotalCPU = 20;
    const ui64 TotalMemory = 16ULL << 30;

    static_assert(KqpRmQueueMemory < TotalMemory);

    auto queue = config.AddQueues();
    queue->SetName(NLocalDb::DefaultQueueName);
    queue->SetWeight(30);
    queue->MutableLimit()->SetCpu(DefaultQueueCPU);

    queue = config.AddQueues();
    queue->SetName("queue_compaction_gen0");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(10);

    queue = config.AddQueues();
    queue->SetName("queue_compaction_gen1");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(6);

    queue = config.AddQueues();
    queue->SetName("queue_compaction_gen2");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);

    queue = config.AddQueues();
    queue->SetName("queue_compaction_gen3");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);

    queue = config.AddQueues();
    queue->SetName("queue_compaction_borrowed");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);

    queue = config.AddQueues();
    queue->SetName("queue_cs_indexation");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);
    queue->MutableLimit()->SetMemory(CSInsertCompactionMemoryLimit);

    queue = config.AddQueues();
    queue->SetName("queue_cs_ttl");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);
    queue->MutableLimit()->SetMemory(CSTTLCompactionMemoryLimit);

    queue = config.AddQueues();
    queue->SetName("queue_cs_general");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);
    queue->MutableLimit()->SetMemory(CSGeneralCompactionMemoryLimit);

    queue = config.AddQueues();
    queue->SetName("queue_cs_scan_read");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);
    queue->MutableLimit()->SetMemory(CSScanMemoryLimit);

    queue = config.AddQueues();
    queue->SetName("queue_cs_normalizer");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(3);
    queue->MutableLimit()->SetMemory(CSScanMemoryLimit);

    queue = config.AddQueues();
    queue->SetName("queue_transaction");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(4);

    queue = config.AddQueues();
    queue->SetName("queue_background_compaction");
    queue->SetWeight(10);
    queue->MutableLimit()->SetCpu(1);

    queue = config.AddQueues();
    queue->SetName("queue_scan");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(10);

    queue = config.AddQueues();
    queue->SetName("queue_backup");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(2);

    queue = config.AddQueues();
    queue->SetName("queue_restore");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(2);

    queue = config.AddQueues();
    queue->SetName(NLocalDb::KqpResourceManagerQueue);
    queue->SetWeight(30);
    queue->MutableLimit()->SetCpu(KqpRmQueueCPU);
    queue->MutableLimit()->SetMemory(KqpRmQueueMemory);

    queue = config.AddQueues();
    queue->SetName("queue_build_index");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(10);

    queue = config.AddQueues();
    queue->SetName("queue_ttl");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(2);

    queue = config.AddQueues();
    queue->SetName("queue_datashard_build_stats");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(1);

    queue = config.AddQueues();
    queue->SetName("queue_cdc_initial_scan");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(4);

    queue = config.AddQueues();
    queue->SetName("queue_statistics_scan");
    queue->SetWeight(100);
    queue->MutableLimit()->SetCpu(1);

    auto task = config.AddTasks();
    task->SetName(NLocalDb::UnknownTaskName);
    task->SetQueueName(NLocalDb::DefaultQueueName);
    task->SetDefaultDuration(TDuration::Minutes(1).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::LegacyQueueIdToTaskName(0));
    task->SetQueueName("queue_compaction_gen0");
    task->SetDefaultDuration(TDuration::Seconds(10).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::LegacyQueueIdToTaskName(1));
    task->SetQueueName("queue_compaction_gen1");
    task->SetDefaultDuration(TDuration::Seconds(30).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::LegacyQueueIdToTaskName(2));
    task->SetQueueName("queue_compaction_gen2");
    task->SetDefaultDuration(TDuration::Minutes(2).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::LegacyQueueIdToTaskName(3));
    task->SetQueueName("queue_compaction_gen3");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("compaction_borrowed");
    task->SetQueueName("queue_compaction_borrowed");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("CS::TTL");
    task->SetQueueName("queue_cs_ttl");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("CS::INDEXATION");
    task->SetQueueName("queue_cs_indexation");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("CS::GENERAL");
    task->SetQueueName("queue_cs_general");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("CS::SCAN_READ");
    task->SetQueueName("queue_cs_scan_read");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("CS::NORMALIZER");
    task->SetQueueName("queue_cs_normalizer");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::TransactionTaskName);
    task->SetQueueName("queue_transaction");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("background_compaction");
    task->SetQueueName("queue_background_compaction");
    task->SetDefaultDuration(TDuration::Minutes(1).GetValue());

    task = config.AddTasks();
    task->SetName("background_compaction_gen0");
    task->SetQueueName("queue_background_compaction");
    task->SetDefaultDuration(TDuration::Seconds(10).GetValue());

    task = config.AddTasks();
    task->SetName("background_compaction_gen1");
    task->SetQueueName("queue_background_compaction");
    task->SetDefaultDuration(TDuration::Seconds(20).GetValue());

    task = config.AddTasks();
    task->SetName("background_compaction_gen2");
    task->SetQueueName("queue_background_compaction");
    task->SetDefaultDuration(TDuration::Minutes(1).GetValue());

    task = config.AddTasks();
    task->SetName("background_compaction_gen3");
    task->SetQueueName("queue_background_compaction");
    task->SetDefaultDuration(TDuration::Minutes(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::ScanTaskName);
    task->SetQueueName("queue_scan");
    task->SetDefaultDuration(TDuration::Minutes(5).GetValue());

    task = config.AddTasks();
    task->SetName("backup");
    task->SetQueueName("queue_backup");
    task->SetDefaultDuration(TDuration::Minutes(5).GetValue());

    task = config.AddTasks();
    task->SetName("restore");
    task->SetQueueName("queue_restore");
    task->SetDefaultDuration(TDuration::Minutes(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName(NLocalDb::KqpResourceManagerQueue);
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("build_index");
    task->SetQueueName("queue_build_index");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("ttl");
    task->SetQueueName("queue_ttl");
    task->SetDefaultDuration(TDuration::Minutes(5).GetValue());

    task = config.AddTasks();
    task->SetName("datashard_build_stats");
    task->SetQueueName("queue_datashard_build_stats");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName("cdc_initial_scan");
    task->SetQueueName("queue_cdc_initial_scan");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    task = config.AddTasks();
    task->SetName("statistics_scan");
    task->SetQueueName("queue_statistics_scan");
    task->SetDefaultDuration(TDuration::Minutes(10).GetValue());

    config.MutableResourceLimit()->SetCpu(TotalCPU);
    config.MutableResourceLimit()->SetMemory(TotalMemory);

    return config;
}

void MergeConfigResources(
        NKikimrResourceBroker::TResources &dst,
        const NKikimrResourceBroker::TResources &src)
{
    // Check if src contains old-style settings
    for (size_t index = 0; index < src.ResourceSize(); ++index) {
        auto value = src.GetResource(index);
        switch (index) {
            case NKikimrResourceBroker::CPU:
                dst.SetCpu(value);
                break;
            case NKikimrResourceBroker::MEMORY:
                dst.SetMemory(value);
                break;
        }
        if (index < dst.ResourceSize()) {
            // Make sure legacy settings are also updated (if they already exist)
            dst.SetResource(index, value);
        }
    }

    // Apply new style cpu settings
    if (src.HasCpu()) {
        dst.SetCpu(src.GetCpu());
        if (dst.ResourceSize() > NKikimrResourceBroker::CPU) {
            // Make sure legacy settings are also updated (if they already exist)
            dst.SetResource(NKikimrResourceBroker::CPU, src.GetCpu());
        }
    }

    // Apply new style memory settings
    if (src.HasMemory()) {
        dst.SetMemory(src.GetMemory());
        if (dst.ResourceSize() > NKikimrResourceBroker::MEMORY) {
            // Make sure legacy settings are also updated (if they already exist)
            dst.SetResource(NKikimrResourceBroker::MEMORY, src.GetMemory());
        }
    }
}

void MergeConfigUpdates(
        NKikimrResourceBroker::TResourceBrokerConfig &config,
        const NKikimrResourceBroker::TResourceBrokerConfig &updates)
{
    THashMap<TString, size_t> queueToIndex;
    for (size_t index = 0; index < config.QueuesSize(); ++index) {
        queueToIndex.emplace(config.GetQueues(index).GetName(), index);
    }
    auto getQueue = [&](const TString &name) -> NKikimrResourceBroker::TQueueConfig& {
        auto it = queueToIndex.find(name);
        if (it != queueToIndex.end()) {
            return *config.MutableQueues(it->second);
        }
        size_t index = config.QueuesSize();
        auto &queue = *config.AddQueues();
        queue.SetName(name);
        queueToIndex.emplace(name, index);
        return queue;
    };

    for (const auto &src : updates.GetQueues()) {
        auto &dst = getQueue(src.GetName());

        if (src.HasWeight()) {
            dst.SetWeight(src.GetWeight());
        }

        if (src.HasLimit()) {
            MergeConfigResources(*dst.MutableLimit(), src.GetLimit());
        }
    }

    THashMap<TString, size_t> taskToIndex;
    for (size_t index = 0; index < config.TasksSize(); ++index) {
        taskToIndex.emplace(config.GetTasks(index).GetName(), index);
    }
    auto getTask = [&](const TString &name) -> NKikimrResourceBroker::TTaskConfig& {
        auto it = taskToIndex.find(name);
        if (it != taskToIndex.end()) {
            return *config.MutableTasks(it->second);
        }
        size_t index = config.TasksSize();
        auto &task = *config.AddTasks();
        task.SetName(name);
        taskToIndex.emplace(name, index);
        return task;
    };

    for (const auto &src : updates.GetTasks()) {
        auto &dst = getTask(src.GetName());

        if (src.HasQueueName()) {
            dst.SetQueueName(src.GetQueueName());
        }

        if (src.HasDefaultDuration()) {
            dst.SetDefaultDuration(src.GetDefaultDuration());
        }
    }

    if (updates.HasResourceLimit()) {
        MergeConfigResources(*config.MutableResourceLimit(), updates.GetResourceLimit());
    }
}


IActor* CreateResourceBrokerActor(const NKikimrResourceBroker::TResourceBrokerConfig &config,
                                  const ::NMonitoring::TDynamicCounterPtr &counters)
{
    return new TResourceBrokerActor(config, counters);
}


} // NResourceBroker
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NResourceBroker::TEvResourceBroker::TStatus::ECode, stream, value) {
    switch (value) {
    case NKikimr::NResourceBroker::TEvResourceBroker::TStatus::ALREADY_EXISTS:
        stream << "ALREADY_EXISTS";
        break;
    case NKikimr::NResourceBroker::TEvResourceBroker::TStatus::UNKNOWN_TASK:
        stream << "UNKNOWN_TASK";
        break;
    case NKikimr::NResourceBroker::TEvResourceBroker::TStatus::TASK_IN_FLY:
        stream << "TASK_IN_FLY";
        break;
    case NKikimr::NResourceBroker::TEvResourceBroker::TStatus::TASK_IN_QUEUE:
        stream << "TASK_IN_QUEUE";
        break;
    default:
        stream << "<UNKNOWN_CODE>";
    }
}
