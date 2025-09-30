#pragma once

#include "benchmark_settings.h"
#include <util/datetime/base.h>

namespace NKikimr {
namespace NMiniKQL {

struct TBenchmarkCaseResult {
    TString CaseName;
    TDuration RunDuration;
};

TVector<TBenchmarkCaseResult> RunJoinsBench(const TBenchmarkSettings& params);

} // namespace NMiniKQL
} // namespace NKikimr