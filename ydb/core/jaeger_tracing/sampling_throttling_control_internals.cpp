#include "sampling_throttling_control_internals.h"


namespace NKikimr::NJaegerTracing {

namespace {

template<class T, class TAction>
void ForEachMatchingRule(TRequestTypeRules<T>& rules, const TMaybe<TString>& database, TAction&& action) {
    for (auto& rule : rules.Global) {
        action(rule);
    }
    if (database) {
        if (auto databaseRules = rules.DatabaseRules.FindPtr(*database)) {
            for (auto& rule : *databaseRules) {
                action(rule);
            }
        }
    }
}

} // namespace anonymous

NWilson::TTraceId TSamplingThrottlingControl::TSamplingThrottlingImpl::HandleTracing(
        TRequestDiscriminator discriminator, const TMaybe<TString>& traceparent) {
    auto requestType = static_cast<size_t>(discriminator.RequestType);
    auto database = std::move(discriminator.Database);
    std::optional<ui8> level;
    NWilson::TTraceId traceId;

    if (traceparent) {
        ForEachMatchingRule(
            Setup.ExternalThrottlingRules[requestType], database,
            [&level](auto& throttlingRule) {
                if (throttlingRule.Throttler->Throttle()) {
                    return;
                }
                if (!level || throttlingRule.Level > *level) {
                    level = throttlingRule.Level;
                }
            }
        );
    }

    if (!level) {
        ForEachMatchingRule(
            Setup.SamplingRules[requestType], database,
            [&level](auto& samplingRule) {
                if (!samplingRule.Sampler.Sample() || samplingRule.Throttler->Throttle()) {
                    return;
                }
                if (!level || samplingRule.Level > *level) {
                    level = samplingRule.Level;
                }
            }
        );
    }

    if (level && !traceId) {
        if (traceparent) {
            // trace can be attached to external span
            traceId = NWilson::TTraceId::FromTraceparentHeader(traceparent.GetRef(), *level);
        } else {
            traceId = NWilson::TTraceId::NewTraceId(*level, Max<ui32>());
        }
    }

    return traceId;
}

} // namespace NKikimr::NJaegerTracing
