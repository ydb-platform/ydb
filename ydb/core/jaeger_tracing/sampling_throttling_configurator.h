#pragma once

#include "sampling_throttling_control.h"

#include "throttler.h"
#include "settings.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr::NJaegerTracing {

class TSamplingThrottlingConfigurator {
public:
    TSamplingThrottlingConfigurator(TIntrusivePtr<ITimeProvider> timeProvider,
                                    TIntrusivePtr<IRandomProvider>& randomProvider);

    TIntrusivePtr<TSamplingThrottlingControl> GetControl();

    void UpdateSettings(TSettings<double, TThrottlingSettings> settings);

private:
    TSettings<double, TIntrusivePtr<TThrottler>> GenerateThrottlers(
        TSettings<double, TThrottlingSettings> settings);
    TSettings<double, TIntrusivePtr<TThrottler>> PropagateUnspecifiedRequest(
        TSettings<double, TIntrusivePtr<TThrottler>> setup);
    
    std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> GenerateSetup();

    TVector<TIntrusivePtr<TSamplingThrottlingControl>> IssuedControls;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TFastRng64 Rng;
    TSettings<double, TIntrusivePtr<TThrottler>> CurrentSettings;  
};

} // namespace NKikimr::NJaegerTracing
