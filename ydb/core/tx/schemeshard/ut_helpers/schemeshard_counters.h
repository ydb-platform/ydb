#pragma once

#include <util/generic/string.h>

namespace NActors {

class TTestActorRuntime;

}

namespace NSchemeShardUT_Private {

using namespace NActors;

ui64 GetSimpleCounter(TTestActorRuntime& runtime, const TString& name);
void CheckSimpleCounter(TTestActorRuntime& runtime, const TString& name, ui64 value);
ui64 GetCumulativeCounter(TTestActorRuntime& runtime, const TString& name);
ui64 GetPercentileCounter(TTestActorRuntime& runtime, const TString& name, const TString& range);
void CheckPercentileCounter(TTestActorRuntime& runtime, const TString& name, const THashMap<TString, ui64>& rangeValues);

}  // namespace NSchemeShardUT_Private
