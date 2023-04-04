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
    , WorkersCount(conveyorSignals->GetCounter("WorkersCount"))
    , WorkersCountLimit(conveyorSignals->GetCounter("WorkersCountLimit"))
    , IncomingRate(conveyorSignals->GetCounter("Incoming", true))
    , SolutionsRate(conveyorSignals->GetCounter("Solved", true)) {

}

void TDistributor::Bootstrap() {
    ALS_NOTICE(NKikimrServices::TX_CONVEYOR) << "conveyor registered: " << SelfId();
    TServiceOperator::Register(Config);
    for (ui32 i = 0; i < Config.GetWorkersCountDef(NKqp::TStagePredictor::GetUsableThreads()); ++i) {
        Workers.emplace_back(Register(new TWorker()));
    }
    WorkersCountLimit->Set(Workers.size());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::TX_CONVEYOR) << "action=processed;owner=" << ev->Get()->GetOwnerId() << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
    SolutionsRate->Inc();
    if (Waiting.size()) {
        Send(ev->Sender, new TEvInternal::TEvNewTask(Waiting.front()));
        Waiting.pop_front();
    } else {
        Workers.emplace_back(ev->Sender);
    }
    if (!*ev->Get()) {
        Send(ev->Get()->GetOwnerId(), new TEvExecution::TEvTaskProcessedResult(ev->Get()->GetErrorMessage()));
    } else {
        Send(ev->Get()->GetOwnerId(), new TEvExecution::TEvTaskProcessedResult(ev->Get()->GetResult()));
    }
    WaitingQueueSize->Set(Waiting.size());
    WorkersCount->Set(Workers.size());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::TX_CONVEYOR) << "action=add_task;owner=" << ev->Sender << ";workers=" << Workers.size() << ";waiting=" << Waiting.size();
    IncomingRate->Inc();
    if (Workers.size()) {
        Send(Workers.back(), new TEvInternal::TEvNewTask(TWorkerTask(ev->Get()->GetTask(), ev->Sender)));
        Workers.pop_back();
    } else if (Waiting.size() < Config.GetQueueSizeLimit()) {
        Waiting.emplace_back(ev->Get()->GetTask(), ev->Sender);
    } else {
        Send(ev->Sender, new TEvExecution::TEvTaskProcessedResult("scan conveyor overloaded (" +
            ::ToString(Waiting.size()) + " > " + ::ToString(Config.GetQueueSizeLimit()) + ")"));
    }
    WaitingQueueSize->Set(Waiting.size());
    WorkersCount->Set(Workers.size());
}

}
