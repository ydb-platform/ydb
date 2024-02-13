#pragma once

#include "sampler.h"
#include "throttler.h"
#include "sampling_throttling_control.h"

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr::NJaegerTracing {

struct TExternalThrottlingRule {
    TIntrusivePtr<TThrottler> Throttler;
};

struct TSamplingRule {
    TSampler Sampler;
    TIntrusivePtr<TThrottler> Throttler;
    ui8 Level;
};

struct TSamplingThrottlingControl::TSamplingThrottlingImpl {
    std::array<TMaybe<TExternalThrottlingRule>,
        static_cast<size_t>(ERequestType::REQUEST_TYPES_CNT)> ExternalThrottlingRules;
    std::array<TStackVec<TSamplingRule, 4>,
        static_cast<size_t>(ERequestType::REQUEST_TYPES_CNT)> SamplingRules;

    void HandleTracing(NWilson::TTraceId& traceId, TRequestDiscriminator discriminator);

private:
    bool Throttle(size_t requestType);

    TMaybe<ui8> Sample(size_t requestType);
};

} // namespace NKikimr::NJaegerTracing
