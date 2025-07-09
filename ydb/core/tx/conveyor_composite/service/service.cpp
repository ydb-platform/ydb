#include "manager.h"
#include "service.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NConveyorComposite {

TDistributor::TDistributor(const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName("COMPOSITE_CONVEYOR")
    , Counters(ConveyorName, conveyorSignals) {
}

TDistributor::~TDistributor() {
}

void TDistributor::Bootstrap() {
    Manager = std::make_unique<TTasksManager>(ConveyorName, Config, SelfId(), Counters);
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())(
        "actor_id", SelfId())("manager", Manager->DebugString());
    Become(&TDistributor::StateMain);
    TBase::Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup(1));
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& evExt) {
    auto& ev = *evExt->Get();
    const TDuration backSendDuration = (TMonotonic::Now() - ev.GetConstructInstant());
    TWorkersPool& workersPool = Manager->MutableWorkersPool(ev.GetWorkersPoolId());
    workersPool.GetCounters()->PackExecuteHistogram->Collect(
        (ev.GetResults().back().GetFinish() - ev.GetResults().front().GetStart()).MicroSeconds());
    workersPool.GetCounters()->PackSizeHistogram->Collect(ev.GetResults().size());
    workersPool.GetCounters()->SendBackHistogram->Collect(backSendDuration.MicroSeconds());
    workersPool.GetCounters()->SendFwdHistogram->Collect(ev.GetForwardSendDuration().MicroSeconds());

    workersPool.GetCounters()->SendBackDuration->Add(backSendDuration.MicroSeconds());
    workersPool.GetCounters()->SendFwdDuration->Add(ev.GetForwardSendDuration().MicroSeconds());

    workersPool.AddDeliveryDuration(ev.GetForwardSendDuration() + backSendDuration);
    workersPool.ReleaseWorker(ev.GetWorkerIdx());
    workersPool.PutTaskResults(ev.DetachResults());
    if (workersPool.HasTasks()) {
        AFL_VERIFY(workersPool.DrainTasks());
    }
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    auto& cat = Manager->MutableCategoryVerified(ev->Get()->GetCategory());
    std::shared_ptr<TProcessScope> scope = cat.UpsertScope(ev->Get()->GetScopeId(), ev->Get()->GetCPULimits());
    cat.RegisterProcess(ev->Get()->GetInternalProcessId(), std::move(scope));
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    auto* evData = ev->Get();
    Manager->MutableCategoryVerified(evData->GetCategory()).UnregisterProcess(evData->GetInternalProcessId());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    const TDuration d = TMonotonic::Now() - ev->Get()->GetConstructInstant();
    Counters.ReceiveTaskDuration->Add(d.MicroSeconds());
    Counters.ReceiveTaskHistogram->Collect(d.MicroSeconds());
    auto& cat = Manager->MutableCategoryVerified(ev->Get()->GetCategory());
    cat.RegisterTask(ev->Get()->GetInternalProcessId(), ev->Get()->DetachTask());
    Y_UNUSED(Manager->DrainTasks());
    cat.GetCounters()->WaitingQueueSize->Set(cat.GetWaitingQueueSize());
}

}   // namespace NKikimr::NConveyorComposite
