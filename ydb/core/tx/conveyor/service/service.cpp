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
    WorkersCount = Config.GetWorkersCountForConveyor(NKqp::TStagePredictor::GetUsableThreads());
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())("actor_id", SelfId())("count", WorkersCount);
    for (ui32 i = 0; i < WorkersCount; ++i) {
        const double usage = Config.GetWorkerCPUUsage(i);
        Workers.emplace_back(Register(new TWorker(ConveyorName, usage, SelfId(), Counters.SendFwdHistogram, Counters.SendFwdDuration)));
        if (usage < 1) {
            AFL_VERIFY(!SlowWorkerId);
            SlowWorkerId = Workers.back();
        }
    }
    AFL_VERIFY(Workers.size())("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())("actor_id", SelfId())("count", WorkersCount);
    Counters.AvailableWorkersCount->Set(Workers.size());
    Counters.WorkersCountLimit->Set(Workers.size());
    Counters.WaitingQueueSizeLimit->Set(Config.GetQueueSizeLimit());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& evExt) {
    auto* ev = evExt->Get();
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "result")("sender", evExt->Sender)
        ("queue", ProcessesOrdered.size())("workers", Workers.size())("count", ev->GetProcessIds().size())("d", ev->GetInstants().back() - ev->GetInstants().front());
    for (ui32 idx = 0; idx < ev->GetProcessIds().size(); ++idx) {
        AddCPUTime(ev->GetProcessIds()[idx], ev->GetInstants()[idx + 1] - std::max(LastAddProcessInstant, ev->GetInstants()[idx]));
    }
    const TDuration dExecution = ev->GetInstants().back() - ev->GetInstants().front();
    Counters.ExecuteHistogram->Collect(dExecution.MicroSeconds());
    Counters.ExecuteDuration->Add(dExecution.MicroSeconds());

    const TMonotonic now = TMonotonic::Now();
    const TDuration dBackSend = now - ev->GetConstructInstant();
    const TDuration dForwardSend = ev->GetForwardSendDuration();

    const TDuration predictedDurationPerTask = std::max<TDuration>(dExecution / ev->GetProcessIds().size(), TDuration::MicroSeconds(10));
    const double alpha = 0.1;
    const ui32 countTheory = (dBackSend + dForwardSend).GetValue() / (alpha * predictedDurationPerTask.GetValue());
    const ui32 countPredicted = std::max<ui32>(1, std::min<ui32>(WaitingTasksCount.Val() / WorkersCount, countTheory));
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "prediction")("alpha", alpha)
        ("send_forward", dForwardSend)("send_back", dBackSend)("count", ev->GetProcessIds().size())("exec", dExecution)("theory_count", countTheory)
        ("real_count", countPredicted);

    Counters.SendBackHistogram->Collect(dBackSend.MicroSeconds());
    Counters.SendBackDuration->Add(dBackSend.MicroSeconds());
    Counters.SolutionsRate->Add(ev->GetProcessIds().size());

    if (ProcessesOrdered.size()) {
        std::vector<TWorkerTask> tasks;
        while (ProcessesOrdered.size() && tasks.size() < countPredicted) {
            auto task = PopTask();
            Counters.WaitingHistogram->Collect((now - task.GetCreateInstant()).MicroSeconds());
            task.OnBeforeStart();
            tasks.emplace_back(std::move(task));
        }
        Counters.PackHistogram->Collect(tasks.size());
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_execute")("id", evExt->Sender)("queue", WaitingTasksCount.Val())("count", tasks.size());
        Send(evExt->Sender, new TEvInternal::TEvNewTask(std::move(tasks)));
    } else {
        AFL_VERIFY(!WaitingTasksCount.Val());
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "return_worker")("id", evExt->Sender);
        Workers.emplace_back(evExt->Sender);
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
    Counters.ReceiveTaskHistogram->Collect((TMonotonic::Now() - ev->Get()->GetConstructInstant()).MicroSeconds());

    TWorkerTask wTask(ev->Get()->GetTask(), itSignal->second, processId);
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "add_task")("proc", processId)("workers", Workers.size())("queue", WaitingTasksCount.Val());

    if (Workers.size()) {
        Counters.WaitingHistogram->Collect(0);

        wTask.OnBeforeStart();
        if (Workers.size() == 1 || !SlowWorkerId || Workers.back() != *SlowWorkerId) {
            Send(Workers.back(), new TEvInternal::TEvNewTask({ wTask }));
            Workers.pop_back();
        } else {
            Send(Workers.front(), new TEvInternal::TEvNewTask({ wTask }));
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
