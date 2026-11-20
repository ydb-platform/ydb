#include "worker.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_CONVEYOR

namespace NKikimr::NConveyor {

TDuration TWorker::GetWakeupDuration() const {
    YDB_LOG_VERIFY(!Instants.empty());
    return (Instants.back() - Instants.front()) * (1 - CPUSoftLimit) / CPUSoftLimit;
}

void TWorker::ExecuteTask(std::vector<TWorkerTask>&& workerTasks) {
    YDB_LOG_VERIFY(ProcessIds.empty() && Instants.empty());
    std::vector<TMonotonic> instants;
    std::vector<ui64> processes;
    processes.reserve(workerTasks.size());
    instants.reserve(workerTasks.size() + 1);
    instants.emplace_back(TMonotonic::Now());
    for (auto&& t : workerTasks) {
        t.GetTask()->Execute(t.GetTaskSignals(), t.GetTask());
        instants.emplace_back(TMonotonic::Now());
        processes.emplace_back(t.GetProcessId());
    }
    if (CPUSoftLimit < 1) {
        YDB_LOG_DEBUG("",
            {"action", "to_wait_result"},
            {"id", SelfId()},
            {"count", workerTasks.size()});
        ProcessIds = std::move(processes);
        Instants = std::move(instants);
        Schedule(GetWakeupDuration(), new NActors::TEvents::TEvWakeup(CPULimitGeneration));
        WaitWakeUp = true;
    } else {
        YDB_LOG_VERIFY(!!ForwardDuration);
        YDB_LOG_DEBUG("",
            {"action", "to_result"},
            {"id", SelfId()},
            {"count", workerTasks.size()},
            {"d", TMonotonic::Now() - instants.front()});
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(instants), std::move(processes), *ForwardDuration, WorkerIdx).SendTo(DistributorId);
        ForwardDuration.reset();
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev) {
    const auto evGeneration = ev->Get()->Tag;
    YDB_LOG_VERIFY(evGeneration <= CPULimitGeneration);
    if (evGeneration == CPULimitGeneration) {
        OnWakeup();
    }
}

void TWorker::OnWakeup() {
    YDB_LOG_DEBUG("",
        {"action", "wake_up"},
        {"id", SelfId()},
        {"count", ProcessIds.size()});
    YDB_LOG_VERIFY(ProcessIds.size());
    YDB_LOG_VERIFY(!!ForwardDuration);
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Instants), std::move(ProcessIds), *ForwardDuration, WorkerIdx).SendTo(DistributorId);
    ForwardDuration.reset();
    ProcessIds.clear();
    Instants.clear();

    WaitWakeUp = false;
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    YDB_LOG_VERIFY(!WaitWakeUp);
    const TMonotonic now = TMonotonic::Now();
    ForwardDuration = now - ev->Get()->GetConstructInstant();
    SendFwdHistogram->Collect(ForwardDuration->MicroSeconds());
    SendFwdDuration->Add(ForwardDuration->MicroSeconds());
    ExecuteTask(ev->Get()->ExtractTasks());
}

void TWorker::HandleMain(TEvInternal::TEvChangeCPUSoftLimit::TPtr& ev) {
    const auto cpuSoftLimit = ev->Get()->GetCPUSoftLimit();
    YDB_LOG_VERIFY(0 < cpuSoftLimit);
    YDB_LOG_VERIFY(cpuSoftLimit <= CPUHardLimit);
    CPUSoftLimit = cpuSoftLimit;

    if (!WaitWakeUp) {
        return;
    }

    auto wakeupTime = Instants.back();
    if (CPUSoftLimit < 1) {
        wakeupTime += GetWakeupDuration();
    }

    CPULimitGeneration++;
    if (wakeupTime > TMonotonic::Now()) {
        Schedule(wakeupTime, new NActors::TEvents::TEvWakeup(CPULimitGeneration));
    } else {
        OnWakeup();
    }
}

}
