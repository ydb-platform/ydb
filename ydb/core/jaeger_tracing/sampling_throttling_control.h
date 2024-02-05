#pragma once

#include "control_wrapper.h"
#include "sampler.h"
#include "throttler.h"
#include "request_discriminator.h"

#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

class TSamplingThrottlingControl
    : public TThrRefBase
    , private TMoveOnly {
    friend class TSamplingThrottlingConfigurator;
    
public:
    void HandleTracing(NWilson::TTraceId& traceId, TRequestDiscriminator discriminator) {
        Y_UNUSED(discriminator);
        if (traceId && ExternalThrottler.Throttle()) {
            traceId = {};
        }
        if (!traceId && Sampler.Sample() && !SampledThrottler.Throttle()) {
            traceId = NWilson::TTraceId::NewTraceId(SampledLevel.Get(), 4095);
        }
    }
    
private:
    // Should only be obtained from TSamplingThrottlingConfigurator
    TSamplingThrottlingControl(
        TSampler sampler,
        TControlWrapper sampledLevel,
        TThrottler sampledThrottler,
        TThrottler externalThrottler
    )
        : Sampler(std::move(sampler))
        , SampledLevel(std::move(sampledLevel))
        , SampledThrottler(std::move(sampledThrottler))
        , ExternalThrottler(std::move(externalThrottler))
    {}

    TSampler Sampler;
    TControlWrapper SampledLevel;
    TThrottler SampledThrottler;
    TThrottler ExternalThrottler;
};

} // namespace NKikimr::NJaegerTracing
