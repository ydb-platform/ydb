#pragma once

#include "sampling_throttling_control.h"

#include "settings.h"
#include "sampler.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr::NJaegerTracing {

class TThrottler;

class TSamplingThrottlingConfigurator {
public:
    TSamplingThrottlingConfigurator(TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider>& randomProvider)
        : TimeProvider(std::move(timeProvider))
        , Rng(randomProvider->GenRand64())
        , CurrentSettings(GenerateThrottlers({}))
    {}

    TIntrusivePtr<TSamplingThrottlingControl> GetControl();

    void UpdateSettings(TSettings<double, TThrottlingSettings> settings);

private:
    TSettings<double, TIntrusivePtr<TThrottler>> GenerateThrottlers(
        TSettings<double, TThrottlingSettings> settings);
    
    std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> GenerateSetup();

    TVector<TIntrusivePtr<TSamplingThrottlingControl>> IssuedControls;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TFastRng64 Rng;
    TSettings<double, TIntrusivePtr<TThrottler>> CurrentSettings;  
};

} // namespace NKikimr::NJaegerTracing
