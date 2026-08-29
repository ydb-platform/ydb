#pragma once

#include <util/generic/string.h>

namespace NActors {

class TTestActorRuntime;
class TTestBasicRuntime;

}

namespace NSchemeShardUT_Private {

using namespace NActors;

ui64 GetSimpleCounter(TTestBasicRuntime& runtime, const TString& name);
void CheckSimpleCounter(TTestBasicRuntime& runtime, const TString& name, ui64 value);
ui64 GetCumulativeCounter(TTestActorRuntime& runtime, const TString& name);
// Executor-level counter (flat_executor_counters.h), not an app counter. Needed
// to observe key/page charge cost, which no app-level counter can see.
ui64 GetExecutorCumulativeCounter(TTestBasicRuntime& runtime, const TString& name);
ui64 GetPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const TString& range);
void CheckPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const THashMap<TString, ui64>& rangeValues);

}  // namespace NSchemeShardUT_Private
