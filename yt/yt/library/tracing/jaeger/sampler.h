#pragma once

#include "public.h"

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TSampler
    : public TRefCounted
{
public:
    TSampler();
    explicit TSampler(
        TSamplerConfigPtr config,
        const NProfiling::TProfiler& profiler = TracingProfiler());

    void SampleTraceContext(const std::string& user, const TTraceContextPtr& traceContext);

    void UpdateConfig(TSamplerConfigPtr config);

private:
    TAtomicIntrusivePtr<TSamplerConfig> Config_;

    NProfiling::TProfiler Profiler_;

    struct TUserState final
    {
        std::atomic<ui64> Sampled = 0;
        std::atomic<NProfiling::TCpuInstant> LastReset = 0;

        bool TrySampleByMinCount(ui64 minCount, NProfiling::TCpuDuration period);

        NProfiling::TCounter TracesSampledByUser;
        NProfiling::TCounter TracesSampledByProbability;
    };

    NConcurrency::TSyncMap<std::string, TIntrusivePtr<TUserState>> Users_;
    NProfiling::TCounter TracesSampled_;
};

DEFINE_REFCOUNTED_TYPE(TSampler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
