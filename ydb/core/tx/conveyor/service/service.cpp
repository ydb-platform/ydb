#include "service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyor {

NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals) {
    return new TDistributor(config, "common", conveyorSignals);
}

TDistributor::TDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName(conveyorName)
    , WaitingQueueSize(conveyorSignals->GetCounter("WaitingQueueSize"))
    , WaitingQueueSizeLimit(conveyorSignals->GetCounter("WaitingQueueSizeLimit"))
    , WorkersCount(conveyorSignals->GetCounter("WorkersCount"))
    , WorkersCountLimit(conveyorSignals->GetCounter("WorkersCountLimit"))
    , IncomingRate(conveyorSignals->GetCounter("Incoming", true))
    , SolutionsRate(conveyorSignals->GetCounter("Solved", true))
    , OverlimitRate(conveyorSignals->GetCounter("Overlimit", true))
{

}

void TDistributor::Bootstrap() {
    const ui32 workersCount = Config.GetWorkersCountDef(NKqp::TStagePredictor::GetUsableThreads());
    ALS_NOTICE(NKikimrServices::TX_CONVEYOR) << "action=conveyor_registered;actor_id=" << SelfId() << ";workers_count=" << workersCount << ";limit=" << Config.GetQueueSizeLimit();
    TServiceOperator::Register(Config);
    for (ui32 i = 0; i < workersCount; ++i) {
        Workers.emplace_back(Register(new TWorker()));
    }
    WorkersCountLimit->Set(Workers.size());
    WaitingQueueSizeLimit->Set(Config.GetQueueSizeLimit());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev) {
    SolutionsRate->Inc();
    if (Waiting.size()) {
        Send(ev->Sender, new TEvInternal::TEvNewTask(Waiting.top()));
        Waiting.pop();
    } else {
        Workers.emplace_back(ev->Sender);
    }
    if (ev->Get()->GetErrorMessage()) {
        ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "action=on_error;owner=" << ev->Get()->GetOwnerId() << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
        Send(ev->Get()->GetOwnerId(), new TEvExecution::TEvTaskProcessedResult(ev->Get()->GetErrorMessage()));
    } else {
        Send(ev->Get()->GetOwnerId(), new TEvExecution::TEvTaskProcessedResult(ev->Get()->GetResult()));
    }
    WaitingQueueSize->Set(Waiting.size());
    WorkersCount->Set(Workers.size());
    ALS_DEBUG(NKikimrServices::TX_CONVEYOR) << "action=processed;owner=" << ev->Get()->GetOwnerId() << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::TX_CONVEYOR) << "action=add_task;owner=" << ev->Sender << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
    IncomingRate->Inc();
    if (Workers.size()) {
        Send(Workers.back(), new TEvInternal::TEvNewTask(TWorkerTask(ev->Get()->GetTask(), ev->Sender)));
        Workers.pop_back();
    } else if (Waiting.size() < Config.GetQueueSizeLimit()) {
        Waiting.emplace(ev->Get()->GetTask(), ev->Sender);
    } else {
        ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "action=overlimit;sender=" << ev->Sender << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
        OverlimitRate->Inc();
        Send(ev->Sender, new TEvExecution::TEvTaskProcessedResult("scan conveyor overloaded (" +
            ::ToString(Waiting.size()) + " >= " + ::ToString(Config.GetQueueSizeLimit()) + ")"));
    }
    WaitingQueueSize->Set(Waiting.size());
    WorkersCount->Set(Workers.size());
}

}
