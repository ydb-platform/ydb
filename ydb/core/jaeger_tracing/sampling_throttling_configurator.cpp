#include "sampler.h"
#include "sampling_throttling_configurator.h"
#include "sampling_throttling_control.h"
#include "throttler.h"

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/string/cast.h>

namespace NKikimr::NJaegerTracing {

TIntrusivePtr<TSamplingThrottlingControl> TSamplingThrottlingConfigurator::GetControl() {
    TSampler sampler(SamplingPPM, RandomProvider->GenRand64());
    TThrottler sampledThrottler(MaxSampledPerMinute, MaxSampledBurst, TimeProvider);
    TThrottler externalThrottler(MaxExternalPerMinute, MaxExternalBurst, TimeProvider);

    return TIntrusivePtr(new TSamplingThrottlingControl(std::move(sampler), SamplingLevel, std::move(sampledThrottler), std::move(externalThrottler)));
}

TMaybe<TString> TSamplingThrottlingConfigurator::HandleConfigs(const NKikimrConfig::TTracingConfig& config) {
    if (config.SamplingSize() > 1) {
        return "Only global scope is supported, thus no more than one sampling scope is allowed";
    }
    if (config.ExternalThrottlingSize() > 1) {
        return "Only global scope is supported, thus no more than one throttling scope is allowed";
    }

    if (config.SamplingSize() == 1) {
        const auto& samplingConfig = config.GetSampling(0);
        if (!(samplingConfig.HasFraction() && samplingConfig.HasLevel() && samplingConfig.HasMaxRatePerMinute())) {
            return "At least one required field (fraction, level, max_rate_per_minute) is missing in scope " + samplingConfig.ShortDebugString();
        }
        const auto samplingFraction = samplingConfig.GetFraction();

        if (samplingFraction < 0 || samplingFraction > 1) {
            return "Sampling fraction violates range [0, 1] " + ToString(samplingFraction);
        }

        SamplingPPM.Set(samplingFraction * 1000000);
        SamplingLevel.Set(samplingConfig.GetLevel());
        MaxSampledPerMinute.Set(samplingConfig.GetMaxRatePerMinute());
        MaxSampledBurst.Set(samplingConfig.GetMaxBurst());
    }

    if (config.ExternalThrottlingSize()) {
        const auto& throttlingConfig = config.externalthrottling(0);
        if (!throttlingConfig.HasMaxRatePerMinute()) {
            return "max_rate_per_minute is missing in this scope: " + throttlingConfig.ShortDebugString();
        }

        MaxExternalPerMinute.Set(throttlingConfig.GetMaxRatePerMinute());
        MaxExternalBurst.Set(throttlingConfig.GetMaxBurst());
    }

    return {};
}

} // namespace NKikimr::NJaegerTracing
