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
    CurrentSettings = GenerateThrottlers(std::move(settings));

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

std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> TSamplingThrottlingConfigurator::GenerateSetup() {
    auto setup = CurrentSettings.MapSampler([this](double fraction) {
        return TSampler(fraction, Rng());
    });

    return std::make_unique<TSamplingThrottlingControl::TSamplingThrottlingImpl>(std::move(setup));
}

} // namespace NKikimr::NJaegerTracing
