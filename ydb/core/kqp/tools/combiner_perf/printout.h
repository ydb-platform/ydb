#pragma once

#include "run_params.h"

#include <util/ysaveload.h>
#include <util/datetime/base.h>
#include <optional>

struct TRunResult
{
    TDuration GeneratorTime = TDuration::Zero();
    TDuration ResultTime = TDuration::Zero();
    TDuration ReferenceTime = TDuration::Zero();
    size_t MaxRSSDelta = 0;
    size_t ReferenceMaxRSSDelta = 0;

    TRunResult()
    {
    }

    TRunResult(TDuration generatorTime, TDuration resultTime, TDuration referenceTime, size_t maxRSSDelta, size_t referenceMaxRSSDelta = -1)
        : GeneratorTime(generatorTime)
        , ResultTime(resultTime)
        , ReferenceTime(referenceTime)
        , MaxRSSDelta(maxRSSDelta)
        , ReferenceMaxRSSDelta(referenceMaxRSSDelta)
    {
    }

    Y_SAVELOAD_DEFINE(GeneratorTime, ResultTime, ReferenceTime, MaxRSSDelta, ReferenceMaxRSSDelta);
};

void MergeRunResults(const TRunResult& src, TRunResult& dst);

class TTestResultCollector {
public:
    virtual void SubmitMetrics(const NKikimr::NMiniKQL::TRunParams& runParams, const TRunResult& result, const char* testName, const std::optional<bool> llvm = {}, const std::optional<bool> spilling = {}) = 0;

    virtual ~TTestResultCollector() {};
};

// ru_maxrss from rusage, converted to bytes
long Y_NO_INLINE GetMaxRSS();

long Y_NO_INLINE GetMaxRSSDelta(const long prevMaxRss);

TDuration Y_NO_INLINE GetThreadCPUTime();

TDuration Y_NO_INLINE GetThreadCPUTimeDelta(const TDuration startTime);