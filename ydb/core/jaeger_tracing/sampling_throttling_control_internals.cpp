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

void TSamplingThrottlingControl::TSamplingThrottlingImpl::HandleTracing(
    NWilson::TTraceId& traceId, TRequestDiscriminator discriminator) {
    auto requestType = static_cast<size_t>(discriminator.RequestType);
    auto database = std::move(discriminator.Database);

    TMaybe<ui8> level;
    if (traceId) {
        level = traceId.GetVerbosity();
        bool throttle = true;

        ForEachMatchingRule(
            Setup.ExternalThrottlingRules[requestType], database,
            [&throttle](auto& throttlingRule) {
                throttle = throttlingRule.Throttler->Throttle() && throttle;
            });

        if (throttle) {
            level = Nothing();
        }
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
            });
    }

    if (level) {
        if (!traceId) {
            traceId = NWilson::TTraceId::NewTraceId(*level, Max<ui32>());
        }
    } else {
        if (traceId) {
            traceId = {};
        }
    }
}

} // namespace NKikimr::NJaegerTracing
