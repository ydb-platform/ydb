#pragma once

#include "control_wrapper.h"
#include "sampling_throttling_control.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/maybe.h>

namespace NKikimr::NJaegerTracing {

class TSamplingThrottlingConfigurator {
public:
    TSamplingThrottlingConfigurator(TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
        : SamplingLevel(15)
        , TimeProvider(std::move(timeProvider))
        , RandomProvider(std::move(randomProvider))
    {}

    TIntrusivePtr<TSamplingThrottlingControl> GetControl();

    TMaybe<TString> HandleConfigs(const NKikimrConfig::TTracingConfig& config);

private:
    TControlWrapper SamplingPPM;
    TControlWrapper SamplingLevel;

    TControlWrapper MaxSampledPerMinute;
    TControlWrapper MaxSampledBurst;

    TControlWrapper MaxExternalPerMinute;
    TControlWrapper MaxExternalBurst;

    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
};

} // namespace NKikimr::NJaegerTracing
