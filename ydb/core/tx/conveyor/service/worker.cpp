#include "worker.h"

namespace NKikimr::NConveyor {

TDuration TWorker::GetWakeupDuration() const {
    AFL_VERIFY(!Instants.empty());
    return (Instants.back() - Instants.front()) * (1 - CPUSoftLimit) / CPUSoftLimit;
}

void TWorker::ExecuteTask(std::vector<TWorkerTask>&& workerTasks) {
    AFL_VERIFY(ProcessIds.empty() && Instants.empty());
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
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_wait_result")("id", SelfId())("count", workerTasks.size());
        ProcessIds = std::move(processes);
        Instants = std::move(instants);
        Schedule(GetWakeupDuration(), new NActors::TEvents::TEvWakeup(CPULimitGeneration));
        WaitWakeUp = true;
    } else {
        AFL_VERIFY(!!ForwardDuration);
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_result")("id", SelfId())("count", workerTasks.size())("d", TMonotonic::Now() - instants.front());
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(instants), std::move(processes), *ForwardDuration, WorkerIdx).SendTo(DistributorId);
        ForwardDuration.reset();
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev) {
    const auto evGeneration = ev->Get()->Tag;
    AFL_VERIFY(evGeneration <= CPULimitGeneration);
    if (evGeneration == CPULimitGeneration) {
        OnWakeup();
    }
}

void TWorker::OnWakeup() {
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "wake_up")("id", SelfId())("count", ProcessIds.size());
    AFL_VERIFY(ProcessIds.size());
    AFL_VERIFY(!!ForwardDuration);
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Instants), std::move(ProcessIds), *ForwardDuration, WorkerIdx).SendTo(DistributorId);
    ForwardDuration.reset();
    ProcessIds.clear();
    Instants.clear();

    WaitWakeUp = false;
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    AFL_VERIFY(!WaitWakeUp);
    const TMonotonic now = TMonotonic::Now();
    ForwardDuration = now - ev->Get()->GetConstructInstant();
    SendFwdHistogram->Collect(ForwardDuration->MicroSeconds());
    SendFwdDuration->Add(ForwardDuration->MicroSeconds());
    ExecuteTask(ev->Get()->ExtractTasks());
}

void TWorker::HandleMain(TEvInternal::TEvChangeCPUSoftLimit::TPtr& ev) {
    const auto cpuSoftLimit = ev->Get()->GetCPUSoftLimit();
    AFL_VERIFY(0 < cpuSoftLimit);
    AFL_VERIFY(cpuSoftLimit <= CPUHardLimit);
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
