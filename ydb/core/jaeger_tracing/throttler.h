#pragma once

#include "control_wrapper.h"

#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NJaegerTracing {

class TThrottler {
public:
    TThrottler(TControlWrapper maxRatePerMinute, TControlWrapper maxBurst,
            TIntrusivePtr<ITimeProvider> timeProvider)
        : TimeProvider(std::move(timeProvider))
        , MaxRatePerMinute(std::move(maxRatePerMinute))
        , MaxBurst(std::move(maxBurst))
        , LastUpdate(TimeProvider->Now())
    {}

    bool Throttle() {
        auto maxRatePerMinute = MaxRatePerMinute.Get();
        auto maxBurst = MaxBurst.Get();
        auto maxTotal = maxBurst + 1;
        CurrentBurst = std::min(CurrentBurst, maxTotal);
        if (maxRatePerMinute == 0) {
            return true;
        }

        auto now = TimeProvider->Now();
        if (now < LastUpdate) {
            return true;
        }

        const auto deltaBetweenSends = TDuration::Minutes(1) / maxRatePerMinute;
        UpdateStats(now, deltaBetweenSends);

        if (CurrentBurst < maxTotal) {
            CurrentBurst += 1;
            return false;
        }

        return true;
    }

private:
    void UpdateStats(TInstant now, TDuration deltaBetweenSends) {
        ui64 decrease = (now - LastUpdate) / deltaBetweenSends;
        decrease = std::min(decrease, CurrentBurst);
        CurrentBurst -= decrease;
        LastUpdate += decrease * deltaBetweenSends;
        if (CurrentBurst == 0) {
            LastUpdate = now;
        }
    }

    TIntrusivePtr<ITimeProvider> TimeProvider;

    TControlWrapper MaxRatePerMinute;
    TControlWrapper MaxBurst;

    TInstant LastUpdate = TInstant::Zero();
    ui64 CurrentBurst = 0;
};

} // namespace NKikimr::NJaegerTracing
