#include "service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyor {

TWorkersPool::TWorkersPool(const TString& conveyorName, const NActors::TActorId& distributorId, const TConfig& config, const TCounters& counters)
    : WorkersCount(config.GetWorkersCountForConveyor(NKqp::TStagePredictor::GetUsableThreads()))
    , Counters(counters) {
    Workers.reserve(WorkersCount);
    for (ui32 i = 0; i < WorkersCount; ++i) {
        const auto usage = config.GetWorkerCPUUsage(i);
        Workers.emplace_back(usage, std::make_unique<TWorker>(conveyorName, usage, distributorId, i, Counters.SendFwdHistogram, Counters.SendFwdDuration));
        MaxWorkerThreads += usage;
    }
    AFL_VERIFY(WorkersCount)("name", conveyorName)("action", "conveyor_registered")("config", config.DebugString())("actor_id", distributorId)("count", WorkersCount);
    Counters.AmountCPULimit->Set(0);
    Counters.AvailableWorkersCount->Set(0);
    Counters.WorkersCountLimit->Set(WorkersCount);
}

bool TWorkersPool::HasFreeWorker() const {
    return !ActiveWorkersIdx.empty();
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch) {
    AFL_VERIFY(HasFreeWorker());
    const auto workerIdx = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters.AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    AFL_VERIFY(workerIdx < ActiveWorkersCount);
    auto& worker = Workers[workerIdx];
    worker.OnStartTask();
    TActivationContext::Send(worker.GetWorkerId(), std::make_unique<TEvInternal::TEvNewTask>(std::move(tasksBatch)));
}

void TWorkersPool::ReleaseWorker(const ui32 workerIdx) {
    AFL_VERIFY(workerIdx < Workers.size());
    Workers[workerIdx].OnStopTask();
    if (workerIdx < ActiveWorkersCount) {
        ActiveWorkersIdx.emplace_back(workerIdx);
        Counters.AvailableWorkersCount->Set(ActiveWorkersIdx.size());
    }
}

void TWorkersPool::ChangeAmountCPULimit(const double delta) {
    AmountCPULimit += delta;
    if (std::abs(AmountCPULimit) < Eps) {
        AmountCPULimit = 0;
    }
    AFL_VERIFY(AmountCPULimit >= 0);
    Counters.AmountCPULimit->Set(AmountCPULimit);
    Counters.ChangeCPULimitRate->Inc();

    double numberThreads = std::min(MaxWorkerThreads, AmountCPULimit);
    if (std::abs(numberThreads - ActiveWorkerThreads) < Eps) {
        return;
    }

    ActiveWorkersCount = 0;
    ActiveWorkerThreads = numberThreads;
    ActiveWorkersIdx.clear();
    for (auto& worker : Workers) {
        if (numberThreads <= 0) {
            break;
        }
        if (!worker.GetRunningTask()) {
            ActiveWorkersIdx.emplace_back(ActiveWorkersCount);
        }
        worker.ChangeCPUSoftLimit(std::min<double>(numberThreads, 1));
        numberThreads -= worker.GetCPUSoftLimit();
        ++ActiveWorkersCount;
    }
    AFL_VERIFY(std::abs(numberThreads) < Eps);
}

TDistributor::TDistributor(const TConfig& config, const TString& conveyorName, const bool enableProcesses, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName(conveyorName)
    , EnableProcesses(enableProcesses)
    , Counters(ConveyorName, conveyorSignals) {

}

TDistributor::~TDistributor() {
    for (const auto& [_, process] : Processes) {
        process.GetCPUGroup()->DecProcesses();
    }
}

void TDistributor::Bootstrap() {
    WorkersPool = std::make_shared<TWorkersPool>(ConveyorName, SelfId(), Config, Counters);
    if (!EnableProcesses) {
        AddProcess(0, TCPULimitsConfig(WorkersPool->GetMaxWorkerThreads(), ""));
    }
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())("actor_id", SelfId())("count", WorkersPool->GetWorkersCount());
    Counters.WaitingQueueSizeLimit->Set(Config.GetQueueSizeLimit());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& evExt) {
    auto* ev = evExt->Get();
    WorkersPool->ReleaseWorker(ev->GetWorketIdx());
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "result")("sender", evExt->Sender)
        ("queue", ProcessesOrdered.size())("count", ev->GetProcessIds().size())("d", ev->GetInstants().back() - ev->GetInstants().front());
    for (ui32 idx = 0; idx < ev->GetProcessIds().size(); ++idx) {
        AddCPUTime(ev->GetProcessIds()[idx], ev->GetInstants()[idx + 1] - std::max(LastAddProcessInstant, ev->GetInstants()[idx]));
        Counters.TaskExecuteHistogram->Collect((ev->GetInstants()[idx + 1] - ev->GetInstants()[idx]).MicroSeconds());
    }
    const TDuration dExecution = ev->GetInstants().back() - ev->GetInstants().front();
    Counters.PackExecuteHistogram->Collect(dExecution.MicroSeconds());
    Counters.ExecuteDuration->Add(dExecution.MicroSeconds());

    const TMonotonic now = TMonotonic::Now();
    const TDuration dBackSend = now - ev->GetConstructInstant();
    const TDuration dForwardSend = ev->GetForwardSendDuration();

    const TDuration predictedDurationPerTask = std::max<TDuration>(dExecution / ev->GetProcessIds().size(), TDuration::MicroSeconds(10));
    const double alpha = 0.1;
    const ui32 countTheory = (dBackSend + dForwardSend).GetValue() / (alpha * predictedDurationPerTask.GetValue());
    const ui32 countPredicted = std::max<ui32>(1, std::min<ui32>(WaitingTasksCount.Val() / WorkersPool->GetWorkersCount(), countTheory));
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "prediction")("alpha", alpha)
        ("send_forward", dForwardSend)("send_back", dBackSend)("count", ev->GetProcessIds().size())("exec", dExecution)("theory_count", countTheory)
        ("real_count", countPredicted);

    Counters.SendBackHistogram->Collect(dBackSend.MicroSeconds());
    Counters.SendBackDuration->Add(dBackSend.MicroSeconds());
    Counters.SolutionsRate->Add(ev->GetProcessIds().size());

    const bool hasFreeWorker = WorkersPool->HasFreeWorker();
    if (ProcessesOrdered.size() && hasFreeWorker) {
        std::vector<TWorkerTask> tasks;
        while (ProcessesOrdered.size() && tasks.size() < countPredicted) {
            auto task = PopTask();
            Counters.WaitingHistogram->Collect((now - task.GetCreateInstant()).MicroSeconds());
            task.OnBeforeStart();
            tasks.emplace_back(std::move(task));
        }
        Counters.PackHistogram->Collect(tasks.size());
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_execute")("id", evExt->Sender)("queue", WaitingTasksCount.Val())("count", tasks.size());
        WorkersPool->RunTask(std::move(tasks));
    } else {
        AFL_VERIFY(!WaitingTasksCount.Val() || !hasFreeWorker);
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "return_worker")("id", evExt->Sender);
    }
    Counters.WaitingQueueSize->Set(WaitingTasksCount.Val());
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    AFL_VERIFY(EnableProcesses);
    auto it = Processes.find(ev->Get()->GetProcessId());
    if (it == Processes.end()) {
        AddProcess(ev->Get()->GetProcessId(), ev->Get()->GetCPULimits());
    } else {
        it->second.IncRegistration();
    }
    Counters.ProcessesCount->Set(Processes.size());
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    AFL_VERIFY(EnableProcesses);
    auto it = Processes.find(ev->Get()->GetProcessId());
    AFL_VERIFY(it != Processes.end());
    if (it->second.DecRegistration()) {
        if (it->second.GetTasks().size()) {
            AFL_VERIFY(ProcessesOrdered.erase(it->second.GetAddress(WorkersPool->GetAmountCPULimit())));
        }
        const auto cpuGroup = it->second.GetCPUGroup();
        Processes.erase(it);
        if (cpuGroup->DecProcesses()) {
            ChangeAmountCPULimit(-cpuGroup->GetCPUThreadsLimit());
            AFL_VERIFY(CPUGroups.erase(cpuGroup->GetName()));
        }
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
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "add_task")("proc", processId)("queue", WaitingTasksCount.Val());

    if (WorkersPool->HasFreeWorker()) {
        Counters.WaitingHistogram->Collect(0);

        wTask.OnBeforeStart();
        WorkersPool->RunTask({ wTask });
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
}

void TDistributor::AddProcess(const ui64 processId, const TCPULimitsConfig& cpuLimits) {
    const auto& groupName = cpuLimits.GetCPUGroupName();
    const auto cpuThreadsLimit = cpuLimits.GetCPUGroupThreadsLimitOptional().value_or(WorkersPool->GetMaxWorkerThreads());
    const auto [it, inserted] = CPUGroups.emplace(groupName, std::make_shared<TCPUGroup>(groupName, cpuThreadsLimit));
    if (inserted) {
        ChangeAmountCPULimit(cpuThreadsLimit);
    } else {
        const auto previousLimit = it->second->GetCPUThreadsLimit();
        it->second->SetCPUThreadsLimit(cpuThreadsLimit);
        ChangeAmountCPULimit(cpuThreadsLimit - previousLimit);
    }
    it->second->IncProcesses();

    ProcessesOrdered.clear();
    AFL_VERIFY(Processes.emplace(processId, TProcess(processId, it->second)).second);
    LastAddProcessInstant = TMonotonic::Now();
    const auto amountCPULimit = WorkersPool->GetAmountCPULimit();
    for (auto&& i : Processes) {
        i.second.CleanCPUMetric();
        if (i.second.GetTasks().size()) {
            ProcessesOrdered.emplace(i.second.GetAddress(amountCPULimit));
        }
    }
}

void TDistributor::AddCPUTime(const ui64 processId, const TDuration d) {
    auto it = Processes.find(processId);
    if (it == Processes.end()) {
        return;
    }
    const auto amountCPULimit = WorkersPool->GetAmountCPULimit();
    if (it->second.GetTasks().size()) {
        AFL_VERIFY(ProcessesOrdered.erase(it->second.GetAddress(amountCPULimit)));
    }
    it->second.AddCPUTime(d);
    if (it->second.GetTasks().size()) {
        AFL_VERIFY(ProcessesOrdered.emplace(it->second.GetAddress(amountCPULimit)).second);
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
        AFL_VERIFY(ProcessesOrdered.emplace(it->second.GetAddress(WorkersPool->GetAmountCPULimit())).second);
    }
    it->second.MutableTasks().push(task);
    WaitingTasksCount.Inc();
}

void TDistributor::ChangeAmountCPULimit(const double delta) {
    if (std::abs(delta) < TWorkersPool::Eps) {
        return;
    }

    WorkersPool->ChangeAmountCPULimit(delta);
    ProcessesOrdered.clear();
    const auto amountCPULimit = WorkersPool->GetAmountCPULimit();
    for (const auto& process : Processes) {
        if (process.second.GetTasks().size()) {
            ProcessesOrdered.emplace(process.second.GetAddress(amountCPULimit));
        }
    }
}

}
