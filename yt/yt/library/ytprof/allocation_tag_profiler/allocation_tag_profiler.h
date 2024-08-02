#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct IAllocationTagProfiler
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(IAllocationTagProfiler);

////////////////////////////////////////////////////////////////////////////////

IAllocationTagProfilerPtr CreateAllocationTagProfiler(
    std::vector<TString> tagNames,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NProfiling::TProfiler profiler = NProfiling::TProfiler{"/memory/heap_usage"});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
