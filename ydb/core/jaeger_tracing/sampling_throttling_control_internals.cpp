#include "sampling_throttling_control_internals.h"


namespace NKikimr::NJaegerTracing {

void TSamplingThrottlingControl::TSamplingThrottlingImpl::HandleTracing(
    NWilson::TTraceId& traceId, const TRequestDiscriminator& discriminator) {
    auto requestType = discriminator.RequestType;

    if (traceId) {
        bool throttle = Throttle(requestType);
        throttle = Throttle(ERequestType::UNSPECIFIED) && throttle;
        if (throttle) {
            traceId = {};
        }
    }

    if (!traceId) {
        TMaybe<ui8> level;
        if (auto sampled_level = Sample(requestType)) {
            level = sampled_level;
        }
        if (auto sampled_level = Sample(ERequestType::UNSPECIFIED)) {
            if (!level || *sampled_level > *level) {
                level = sampled_level;
            }
        }

        if (level) {
            traceId = NWilson::TTraceId::NewTraceId(*level, Max<ui32>());
        }
    }
}

bool TSamplingThrottlingControl::TSamplingThrottlingImpl::Throttle(ERequestType requestType) {
    auto& throttlingRule = Setup.ExternalThrottlingRules[static_cast<size_t>(requestType)];
    if (throttlingRule) {
        return throttlingRule->Throttler->Throttle();
    } else {
        return true;
    }
}

TMaybe<ui8> TSamplingThrottlingControl::TSamplingThrottlingImpl::Sample(ERequestType requestType) {
    TMaybe<ui8> level;
    for (auto& samplingRule : Setup.SamplingRules[static_cast<size_t>(requestType)]) {
        if (samplingRule.Sampler.Sample() && !samplingRule.Throttler->Throttle()) {
            if (!level || *level < samplingRule.Level) {
                level = samplingRule.Level;
            }
        }
    }
    return level;
}

} // namespace NKikimr::NJaegerTracing
