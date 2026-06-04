#pragma once

#include <util/generic/string.h>

namespace NActors {

class TTestBasicRuntime;

}

namespace NSchemeShardUT_Private {

using namespace NActors;

ui64 GetSimpleCounter(TTestBasicRuntime& runtime, const TString& name);
void CheckSimpleCounter(TTestBasicRuntime& runtime, const TString& name, ui64 value);
ui64 GetCumulativeCounter(TTestBasicRuntime& runtime, const TString& name);
ui64 GetPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const TString& range);
void CheckPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const THashMap<TString, ui64>& rangeValues);

}  // namespace NSchemeShardUT_Private
