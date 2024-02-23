#include "sampling_throttling_control_internals.h"


namespace NKikimr::NJaegerTracing {

void TSamplingThrottlingControl::TSamplingThrottlingImpl::HandleTracing(
    NWilson::TTraceId& traceId,  TRequestDiscriminator discriminator) {
    auto requestType = static_cast<size_t>(discriminator.RequestType);
    auto database = std::move(discriminator.Database);

    if (traceId) {
        bool throttle = Throttle(requestType, database);
        if (database) {
            throttle = Throttle(requestType, NothingObject) && throttle;
        }
        if (throttle) {
            traceId = {};
        }
    }

    if (!traceId) {
        TMaybe<ui8> level;
        if (auto sampled_level = Sample(requestType, database)) {
            level = sampled_level;
        }
        if (database) {
            if (auto sampled_level = Sample(requestType, NothingObject)) {
                if (!level || *sampled_level > *level) {
                    level = sampled_level;
                }
            }
        }

        if (level) {
            traceId = NWilson::TTraceId::NewTraceId(*level, Max<ui32>());
        }
    }
}

bool TSamplingThrottlingControl::TSamplingThrottlingImpl::Throttle(
    size_t requestType, const TMaybe<TString>& database) {
    bool throttle = true;

    auto& requestTypeThrottlingRules = Setup.ExternalThrottlingRules[requestType];
    if (auto it = requestTypeThrottlingRules.FindPtr(database)) {
        for (auto& throttlingRule : *it) {
            throttle = throttlingRule.Throttler->Throttle() && throttle;
        }
    }
    return throttle;
}

TMaybe<ui8> TSamplingThrottlingControl::TSamplingThrottlingImpl::Sample(
    size_t requestType, const TMaybe<TString>& database) {
    TMaybe<ui8> level;
    auto& requestTypeThrottlingRules = Setup.SamplingRules[requestType];
    if (auto it = requestTypeThrottlingRules.FindPtr(database)) {
        for (auto& samplingRule : *it) {
            if (samplingRule.Sampler.Sample() && !samplingRule.Throttler->Throttle()) {
                auto sampledLevel = samplingRule.Level;
                if (!level || sampledLevel > *level) {
                    level = sampledLevel;
                }
            }
        }
    }
    return level;
}

} // namespace NKikimr::NJaegerTracing
