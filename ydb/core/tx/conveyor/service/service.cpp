#include "service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyor {

TDistributor::TDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName(conveyorName)
    , Counters(ConveyorName, conveyorSignals) {

}

void TDistributor::Bootstrap() {
    AddProcess(0);
    const ui32 workersCount = Config.GetWorkersCountForConveyor(NKqp::TStagePredictor::GetUsableThreads());
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())("actor_id", SelfId());
    for (ui32 i = 0; i < workersCount; ++i) {
        const double usage = Config.GetWorkerCPUUsage(i);
        Workers.emplace_back(Register(new TWorker(ConveyorName, usage, SelfId())));
        if (usage < 1) {
            AFL_VERIFY(!SlowWorkerId);
            SlowWorkerId = Workers.back();
        }
    }
    Counters.AvailableWorkersCount->Set(Workers.size());
    Counters.WorkersCountLimit->Set(Workers.size());
    Counters.WaitingQueueSizeLimit->Set(Config.GetQueueSizeLimit());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev) {
    const auto now = TMonotonic::Now();
    const TDuration dExecution = now - ev->Get()->GetStartInstant();
    Counters.SolutionsRate->Inc();
    Counters.ExecuteHistogram->Collect(dExecution.MilliSeconds());
    AddCPUTime(ev->Get()->GetProcessId(), now - std::max(LastAddProcessInstant, ev->Get()->GetStartInstant()));
    if (ProcessesOrdered.size()) {
        auto task = PopTask();
        Counters.WaitingHistogram->Collect((now - task.GetCreateInstant()).MilliSeconds());
        task.OnBeforeStart();
        Send(ev->Sender, new TEvInternal::TEvNewTask(task));
    } else {
        Workers.emplace_back(ev->Sender);
    }
    Counters.WaitingQueueSize->Set(WaitingTasksCount.Val());
    Counters.AvailableWorkersCount->Set(Workers.size());
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    auto it = Processes.find(ev->Get()->GetProcessId());
    if (it == Processes.end()) {
        AddProcess(ev->Get()->GetProcessId());
    } else {
        it->second.IncRegistration();
    }
    Counters.ProcessesCount->Set(Processes.size());
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    auto it = Processes.find(ev->Get()->GetProcessId());
    AFL_VERIFY(it != Processes.end());
    if (it->second.DecRegistration()) {
        if (it->second.GetTasks().size()) {
            AFL_VERIFY(ProcessesOrdered.erase(it->second.GetAddress()));
        }
        Processes.erase(it);
    }
    Counters.ProcessesCount->Set(Processes.size());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    Counters.IncomingRate->Inc();
    const ui64 processId = ev->Get()->GetProcessId();
    const TString taskClass = ev->Get()->GetTask()->GetTaskClassIdentifier();
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "add_task")("sender", ev->Sender)("task", taskClass);
    auto itSignal = Signals.find(taskClass);
    if (itSignal == Signals.end()) {
        itSignal = Signals.emplace(taskClass, std::make_shared<TTaskSignals>("Conveyor/" + ConveyorName, taskClass)).first;
    }

    TWorkerTask wTask(ev->Get()->GetTask(), itSignal->second, processId);

    if (Workers.size()) {
        Counters.WaitingHistogram->Collect(0);

        wTask.OnBeforeStart();
        if (Workers.size() == 1 || !SlowWorkerId || Workers.back() != *SlowWorkerId) {
            Send(Workers.back(), new TEvInternal::TEvNewTask(wTask));
            Workers.pop_back();
        } else {
            Send(Workers.front(), new TEvInternal::TEvNewTask(wTask));
            Workers.pop_front();
        }
        Counters.UseWorkerRate->Inc();
    } else if (WaitingTasksCount.Val() < Config.GetQueueSizeLimit()) {
        PushTask(wTask);
        Counters.WaitWorkerRate->Inc();
    } else {
        Counters.OverlimitRate->Inc();
        AFL_ERROR(NKikimrServices::TX_CONVEYOR)("action", "queue_overlimit")("sender", ev->Sender)("limit", Config.GetQueueSizeLimit());
        ev->Get()->GetTask()->OnCannotExecute("scan conveyor overloaded (" + ::ToString(WaitingTasksCount.Val()) + " >= " + ::ToString(Config.GetQueueSizeLimit()) + ")");
    }
    Counters.WaitingQueueSize->Set(WaitingTasksCount.Val());
    Counters.AvailableWorkersCount->Set(Workers.size());
}

void TDistributor::AddProcess(const ui64 processId) {
    ProcessesOrdered.clear();
    AFL_VERIFY(Processes.emplace(processId, TProcess(processId)).second);
    LastAddProcessInstant = TMonotonic::Now();
    for (auto&& i : Processes) {
        i.second.CleanCPUMetric();
        if (i.second.GetTasks().size()) {
            ProcessesOrdered.emplace(i.second.GetAddress());
        }
    }
}

void TDistributor::AddCPUTime(const ui64 processId, const TDuration d) {
    auto it = Processes.find(processId);
    if (it == Processes.end()) {
        return;
    }
    if (it->second.GetTasks().size()) {
        AFL_VERIFY(ProcessesOrdered.erase(it->second.GetAddress()));
    }
    it->second.AddCPUTime(d);
    if (it->second.GetTasks().size()) {
        AFL_VERIFY(ProcessesOrdered.emplace(it->second.GetAddress()).second);
    }
}

TWorkerTask TDistributor::PopTask() {
    AFL_VERIFY(ProcessesOrdered.size());
    auto it = Processes.find(ProcessesOrdered.begin()->GetProcessId());
    AFL_VERIFY(it != Processes.end());
    AFL_VERIFY(it->second.GetTasks().size());
    WaitingTasksCount.Dec();
    if (it->second.GetTasks().size() == 1) {
        ProcessesOrdered.erase(ProcessesOrdered.begin());
    }
    return it->second.MutableTasks().pop();
}

void TDistributor::PushTask(const TWorkerTask& task) {
    auto it = Processes.find(task.GetProcessId());
    AFL_VERIFY(it != Processes.end());
    if (it->second.GetTasks().size() == 0) {
        AFL_VERIFY(ProcessesOrdered.emplace(it->second.GetAddress()).second);
    }
    it->second.MutableTasks().push(task);
    WaitingTasksCount.Inc();
}

}
