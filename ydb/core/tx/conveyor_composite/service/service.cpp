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

void TDistributor::HandleMain(NActors::TEvents::TEvWakeup::TPtr& evExt) {
    if (evExt->Get()->Tag == 1) {
        Manager->DoQuant(TMonotonic::Now());
        TBase::Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup(1));
    }
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& evExt) {
    TWorkersPool& workersPool = Manager->MutableWorkersPool(evExt->Get()->GetWorkersPoolId());
    workersPool.AddDeliveryDuration(evExt->Get()->GetForwardSendDuration() + (TMonotonic::Now() - evExt->Get()->GetConstructInstant()));
    workersPool.ReleaseWorker(evExt->Get()->GetWorkerIdx());
    for (auto&& i : evExt->Get()->DetachResults()) {
        workersPool.PutTaskResult(std::move(i));
    }
    if (workersPool.HasTasks()) {
        AFL_VERIFY(workersPool.DrainTasks());
    }
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    Manager->MutableCategoryVerified(ev->Get()->GetCategory())
        .UpsertScope(ev->Get()->GetScopeId(), ev->Get()->GetCPULimits())
        .RegisterProcess(ev->Get()->GetProcessId());
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    auto* evData = ev->Get();
    if (Manager->MutableCategoryVerified(evData->GetCategory())
            .MutableProcessScope(evData->GetScopeId())
            .UnregisterProcess(evData->GetProcessId())) {
        Manager->MutableCategoryVerified(evData->GetCategory()).UnregisterScope(evData->GetScopeId());
    }
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    auto& cat = Manager->MutableCategoryVerified(ev->Get()->GetCategory());
    cat.MutableProcessScope(ev->Get()->GetScopeId())
        .MutableProcessVerified(ev->Get()->GetProcessId())
        .RegisterTask(ev->Get()->GetTask(), ev->Get()->GetScopeId(), cat.GetCounters());
    Y_UNUSED(Manager->DrainTasks());
    cat.GetCounters()->WaitingQueueSize->Set(cat.GetWaitingQueueSize());
}

}   // namespace NKikimr::NConveyorComposite
