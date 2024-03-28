#include "sampling_throttling_configurator.h"

#include "sampling_throttling_control.h"
#include "sampling_throttling_control_internals.h"

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/string/cast.h>

namespace NKikimr::NJaegerTracing {

namespace {

template<class T>
void PropagateUnspecifiedRequest(TRulesContainer<T>& rules) {
    constexpr auto unspecifiedRequestType = static_cast<size_t>(ERequestType::UNSPECIFIED);
    const auto& unspecifiedRequestTypeRules = rules[unspecifiedRequestType];
    
    for (size_t requestType = 0; requestType < kRequestTypesCnt; ++requestType) {
        if (requestType == unspecifiedRequestType) {
            continue;
        }

        auto& requestTypeDatabaseRules = rules[requestType].DatabaseRules;
        auto& requestTypeGlobalRules = rules[requestType].Global;
        for (const auto& [database, unspecifiedDatabaseRules] : unspecifiedRequestTypeRules.DatabaseRules) {
            auto& databaseRules = requestTypeDatabaseRules[database];
            databaseRules.insert(databaseRules.end(), unspecifiedDatabaseRules.begin(),
                                         unspecifiedDatabaseRules.end());
        }
        requestTypeGlobalRules.insert(requestTypeGlobalRules.end(),
                                      unspecifiedRequestTypeRules.Global.begin(),
                                      unspecifiedRequestTypeRules.Global.end());
    }
}

} // namespace anonymous

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

void TSamplingThrottlingConfigurator::UpdateSettings(TSettings<double, TWithTag<TThrottlingSettings>> settings) {
    auto enrichedSettings = GenerateThrottlers(std::move(settings));
    PropagateUnspecifiedRequest(enrichedSettings.SamplingRules);
    PropagateUnspecifiedRequest(enrichedSettings.ExternalThrottlingRules);
    CurrentSettings = std::move(enrichedSettings);

    for (auto& control : IssuedControls) {
        control->UpdateImpl(GenerateSetup());
    }
}

TSettings<double, TIntrusivePtr<TThrottler>> TSamplingThrottlingConfigurator::GenerateThrottlers(
    TSettings<double, TWithTag<TThrottlingSettings>> settings) {
    THashMap<size_t, TIntrusivePtr<TThrottler>> throttlers;
    return settings.MapThrottler([this, &throttlers](const TWithTag<TThrottlingSettings>& settings) {
        if (auto it = throttlers.FindPtr(settings.Tag)) {
            return *it;
        }
        auto throttler = MakeIntrusive<TThrottler>(settings.Value.MaxTracesPerMinute, settings.Value.MaxTracesBurst, TimeProvider);
        throttlers[settings.Tag] = throttler;
        return throttler;
    });
}

std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> TSamplingThrottlingConfigurator::GenerateSetup() {
    auto setup = CurrentSettings.MapSampler([this](double fraction) {
        return TSampler(fraction, Rng());
    });

    return std::make_unique<TSamplingThrottlingControl::TSamplingThrottlingImpl>(std::move(setup));
}

} // namespace NKikimr::NJaegerTracing
