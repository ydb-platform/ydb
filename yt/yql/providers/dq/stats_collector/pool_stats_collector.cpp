#include "pool_stats_collector.h"

#include <ydb/library/actors/helpers/pool_stats_collector.h>

namespace NYql {

using namespace NActors;
using namespace NMonitoring;

namespace {

TIntrusivePtr<TDynamicCounters> GetServiceCounters(TIntrusivePtr<TDynamicCounters> root,
                                                   const TString &service)
{
    auto res = root->GetSubgroup("counters", service);
    auto utils = root->GetSubgroup("counters", "utils");
    auto lookupCounter = utils->GetSubgroup("component", service)->GetCounter("CounterLookups", true);
    res->SetLookupCounter(lookupCounter);
    return res;
}

}

IActor *CreateStatsCollector(ui32 intervalSec,
                             const TActorSystemSetup& setup,
                             NMonitoring::TDynamicCounterPtr counters)
{
    return new NActors::TStatsCollectingActor(intervalSec, setup, GetServiceCounters(counters, "utils"));
}

} // namespace NKikimr
