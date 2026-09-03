#pragma once

#include "benchmark_settings.h"
#include <functional>
#include <util/datetime/base.h>

namespace NKikimr {
namespace NMiniKQL {

struct TBenchmarkCaseResult {
    TString CaseName;
    // The case's own coordinates, repeated here so that a report is readable
    // without parsing CaseName apart.
    ETestedJoinAlgo Algo{};
    TKeySchema KeySchema;
    ETestedPayload Payload{};
    ETestedInputFlavour Flavour{};
    // Median CPU time over all samples. Median rather than mean because a single
    // descheduled run skews the mean enough to hide the few percent that separate
    // two implementations.
    TDuration RunDuration;
    TDuration MinCpu;
    TDuration MaxCpu;
    TDuration MeanCpu;
    TDuration StdevCpu;
    TDuration MedianWall;
    double CvPercent = 0.0;
    int Samples = 0;
    int ItersPerSample = 0;
    i64 OutputRows = 0;
    TTableSizes Sizes{};
    TSelectivity Selectivity{};
    EJoinKind JoinKind = EJoinKind::Inner;
    ETestedFilter Filter = ETestedFilter::kNone;
    // Number of input blocks each side was fed, zero for the row based
    // algorithms. Reported because arrow chunking can split a requested block
    // length into several blocks.
    i64 LeftBlocks = 0;
    i64 RightBlocks = 0;
};

using TBenchmarkResultConsumer = std::function<void(const TBenchmarkCaseResult&)>;

void RunJoinsBench(const TBenchmarkSettings& params, const TBenchmarkResultConsumer& consume);

} // namespace NMiniKQL
} // namespace NKikimr
