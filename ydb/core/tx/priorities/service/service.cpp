#include "service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NPrioritiesQueue {

void TDistributor::Bootstrap() {
    Counters->Limit->Set(Config.GetLimit());
    Manager = std::make_unique<TManager>(Counters, Config, SelfId());
    Become(&TDistributor::StateMain);
}

TDistributor::TDistributor(const TConfig& config, const TString& queueName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
    : Counters(std::make_shared<TCounters>(queueName, baseSignals))
    , QueueName(queueName)
    , Config(config) {
}

}
