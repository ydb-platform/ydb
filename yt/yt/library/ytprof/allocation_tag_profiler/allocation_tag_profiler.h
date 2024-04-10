#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

class THeapUsageProfiler
    : public TRefCounted
{
public:
    THeapUsageProfiler(
        std::vector<TString> tags,
        IInvokerPtr invoker,
        std::optional<TDuration> updatePeriod,
        std::optional<i64> samplingRate,
        NProfiling::TProfiler profiler);

private:
    NProfiling::TProfiler Profiler_;
    const std::vector<TString> TagTypes_;
    THashMap<TString, THashMap<TString, NProfiling::TGauge>> HeapUsageByType_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    void UpdateGauges();
};

DEFINE_REFCOUNTED_TYPE(THeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

THeapUsageProfilerPtr CreateHeapProfilerWithTags(
    std::vector<TString>&& tags,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NProfiling::TProfiler profiler = NProfiling::TProfiler{"/heap_usage/"});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
