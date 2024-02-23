#include "sampling_throttling_configurator.h"

#include "sampling_throttling_control.h"
#include "sampling_throttling_control_internals.h"

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/string/cast.h>

namespace NKikimr::NJaegerTracing {

TSamplingThrottlingConfigurator::TSamplingThrottlingConfigurator(TIntrusivePtr<ITimeProvider> timeProvider,
                                                                 TIntrusivePtr<IRandomProvider>& randomProvider)
    : TimeProvider(std::move(timeProvider))
    , Rng(randomProvider->GenRand64())
    , CurrentSettings(GenerateThrottlers({}))
{}

TIntrusivePtr<TSamplingThrottlingControl> TSamplingThrottlingConfigurator::GetControl() {
    auto control = TIntrusivePtr(new TSamplingThrottlingControl(GenerateSetup()));
    IssuedControls.push_back(control);
    return control;
}

void TSamplingThrottlingConfigurator::UpdateSettings(TSettings<double, TThrottlingSettings> settings) {
    auto enrichedSettings = GenerateThrottlers(std::move(settings));
    CurrentSettings = PropagateUnspecifiedRequest(std::move(enrichedSettings));

    for (auto& control : IssuedControls) {
        control->UpdateImpl(GenerateSetup());
    }
}

TSettings<double, TIntrusivePtr<TThrottler>> TSamplingThrottlingConfigurator::GenerateThrottlers(
    TSettings<double, TThrottlingSettings> settings) {
    return settings.MapThrottler([this](const TThrottlingSettings& settings) {
        return MakeIntrusive<TThrottler>(settings.MaxTracesPerMinute, settings.MaxTracesBurst, TimeProvider);
    });
}

TSettings<double, TIntrusivePtr<TThrottler>> TSamplingThrottlingConfigurator::PropagateUnspecifiedRequest(
    TSettings<double, TIntrusivePtr<TThrottler>> setup) {
    auto unspecifiedRequestType = static_cast<size_t>(ERequestType::UNSPECIFIED);

    for (size_t requestType = 0; requestType < kRequestTypesCnt; ++requestType) {
        if (requestType == unspecifiedRequestType) {
            continue;
        }

        auto& requestTypeSamplingRules = setup.SamplingRules[requestType];
        for (const auto& [database, rules] : setup.SamplingRules[unspecifiedRequestType]) {
            auto& databaseSamplingRules = requestTypeSamplingRules[database];
            databaseSamplingRules.insert(databaseSamplingRules.end(), rules.begin(), rules.end());
        }

        auto& requestTypeThrottlingRules = setup.ExternalThrottlingRules[requestType];
        for (const auto& [database, rules] : setup.ExternalThrottlingRules[unspecifiedRequestType]) {
            auto& databaseThrottlingRules = requestTypeThrottlingRules[database];
            databaseThrottlingRules.insert(databaseThrottlingRules.end(), rules.begin(), rules.end());
        }
    }
    return setup;
}

std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> TSamplingThrottlingConfigurator::GenerateSetup() {
    auto setup = CurrentSettings.MapSampler([this](double fraction) {
        return TSampler(fraction, Rng());
    });

    return std::make_unique<TSamplingThrottlingControl::TSamplingThrottlingImpl>(std::move(setup));
}

} // namespace NKikimr::NJaegerTracing
