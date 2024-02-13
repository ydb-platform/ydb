#include "sampling_throttling_control_internals.h"


namespace NKikimr::NJaegerTracing {

void TSamplingThrottlingControl::TSamplingThrottlingImpl::HandleTracing(
    NWilson::TTraceId& traceId, TRequestDiscriminator discriminator) {
    auto requestType = static_cast<size_t>(discriminator.RequestType);

    if (traceId) {
        bool throttle = true;
        if (!Throttle(requestType)) {
            throttle = false;
        }
        if (throttle && !Throttle(static_cast<size_t>(ERequestType::UNSPECIFIED))) {
            throttle = false;
        }
        if (throttle) {
            traceId = {};
        }
    }

    if (!traceId) {
        TMaybe<ui8> level;
        if (auto sampled_level = Sample(requestType)) {
            level = sampled_level;
        }
        if (auto sampled_level = Sample(requestType)) {
            if (!level || *sampled_level > *level) {
                level = sampled_level;
            }
        }

        if (level) {
            traceId = NWilson::TTraceId::NewTraceId(*level, Max<ui32>());
        }
    }
}

bool TSamplingThrottlingControl::TSamplingThrottlingImpl::Throttle(size_t requestType) {
    auto& rule = ExternalThrottlingRules[requestType];
    if (rule) {
        return rule->Throttler->Throttle();
    } else {
        return false;
    }
}

TMaybe<ui8> TSamplingThrottlingControl::TSamplingThrottlingImpl::Sample(size_t requestType) {
    TMaybe<ui8> level;
    for (auto& rule : SamplingRules[requestType]) {
        if (rule.Sampler.Sample() && !rule.Throttler->Throttle()) {
            if (!level || *level < rule.Level) {
                level = rule.Level;
            }
        }
    }
    return level;
}

} // namespace NKikimr::NJaegerTracing
