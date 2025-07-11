#pragma once

#include "sampling_throttling_control.h"

#include "throttler.h"
#include "settings.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NKikimr::NJaegerTracing {

// Used to represent shared limits in throttlers and samplers
template<class T>
struct TWithTag {
    T Value;
    size_t Tag;
};

class TSamplingThrottlingConfigurator: public TRefCounted<TSamplingThrottlingConfigurator, TAtomicCounter> {
public:
    TSamplingThrottlingConfigurator(TIntrusivePtr<ITimeProvider> timeProvider,
                                    TIntrusivePtr<IRandomProvider>& randomProvider);

    TIntrusivePtr<TSamplingThrottlingControl> GetControl();

    void UpdateSettings(TSettings<double, TWithTag<TThrottlingSettings>> settings);

private:
    TSettings<double, TIntrusivePtr<TThrottler>> GenerateThrottlers(
        TSettings<double, TWithTag<TThrottlingSettings>> settings) const;

    std::unique_ptr<TSamplingThrottlingControl::TSamplingThrottlingImpl> GenerateSetup();

    TVector<TIntrusivePtr<TSamplingThrottlingControl>> IssuedControls;
    const TIntrusivePtr<ITimeProvider> TimeProvider;
    TFastRng64 Rng;
    TSettings<double, TIntrusivePtr<TThrottler>> CurrentSettings;
    TMutex ControlMutex;
};

} // namespace NKikimr::NJaegerTracing
