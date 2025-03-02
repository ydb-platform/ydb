#include "actor_distributor_service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyor {

TActorDistributor::TActorDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> /* conveyorCounters */)
    : Config(config)
    , ConveyorName(conveyorName) {
}

void TActorDistributor::Bootstrap() {
    const ui32 workersCount = Config.GetWorkersCountForConveyor(NKqp::TStagePredictor::GetUsableThreads());
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString())("actor_id", SelfId());
    for (ui32 i = 0; i < workersCount; ++i) {
        Workers.Add(Register(new TActorWorker(ConveyorName,  SelfId())));
    }
    Become(&TActorDistributor::StateMain);
}

void TActorDistributor::HandleMain(TEvExecution::TEvRegisterActor::TPtr& ev) {
    auto worker = Workers.Top();
    Send(ev->Forward(worker));
    Workers.ChangeLoad(worker, +1);
}

void TActorDistributor::HandleMain(TEvExecution::TEvActorFinished::TPtr& ev) {
    auto actor = ev.Get()->Get()->GetActorId();
    auto inflightIt = InFlightActors.find(actor);
    if (inflightIt == InFlightActors.end()) {
        // log
        return;
    }
    auto worker = inflightIt->second;
    Workers.ChangeLoad(worker, -1);
}


}
