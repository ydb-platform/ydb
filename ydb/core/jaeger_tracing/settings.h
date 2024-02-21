#pragma once

#include "request_discriminator.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/system/types.h>

namespace NKikimr::NJaegerTracing {

struct TThrottlingSettings {
    ui64 MaxRatePerMinute;
    ui64 MaxBurst;
};

template<class TSampling, class TThrottling>
struct TSamplingRule {
    ui8 Level;
    TSampling Sampler;
    TThrottling Throttler;
};

template<class TThrottling>
struct TExternalThrottlingRule {
    TThrottling Throttler;
};

template<class TSampling, class TThrottling>
struct TSettings {
    std::array<TStackVec<TSamplingRule<TSampling, TThrottling>, 2>, kRequestTypesCnt> SamplingRules;
    std::array<TMaybe<TExternalThrottlingRule<TThrottling>>, kRequestTypesCnt> ExternalThrottlingRules;

    template<class TFunc>
    auto MapSampler(TFunc&& f) const {
        using TNewSamplingType = std::invoke_result_t<TFunc, const TSampling&>;

        TSettings<TNewSamplingType, TThrottling> newSettings;
        for (size_t i = 0; i < kRequestTypesCnt; ++i) {
            for (auto& samplingRule : SamplingRules[i]) {
                newSettings.SamplingRules[i].push_back(TSamplingRule<TNewSamplingType, TThrottling> {
                    .Level = samplingRule.Level,
                    .Sampler = f(samplingRule.Sampler),
                    .Throttler = samplingRule.Throttler,
                });
            }
            newSettings.ExternalThrottlingRules[i] = ExternalThrottlingRules[i];
        }

        return newSettings;
    }

    template<class TFunc>
    auto MapThrottler(TFunc&& f) const {
        using TNewThrottlingType = std::invoke_result_t<TFunc, const TThrottling&>;

        TSettings<TSampling, TNewThrottlingType> newSettings;
        for (size_t i = 0; i < kRequestTypesCnt; ++i) {
            for (auto& samplingRule : SamplingRules[i]) {
                newSettings.SamplingRules[i].push_back(TSamplingRule<TSampling, TNewThrottlingType> {
                    .Level = samplingRule.Level,
                    .Sampler = samplingRule.Sampler,
                    .Throttler = f(samplingRule.Throttler),
                });
            }
            newSettings.ExternalThrottlingRules[i] = ExternalThrottlingRules[i].Transform(
                [&f](const TExternalThrottlingRule<TThrottling>& rule) {
                    return TExternalThrottlingRule<TNewThrottlingType> {
                        .Throttler = f(rule.Throttler),
                    };
                }
            );
        }

        return newSettings;
    }
};

} // namespace NKikimr::NJaegerTracing
