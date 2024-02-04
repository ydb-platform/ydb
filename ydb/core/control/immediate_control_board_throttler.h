#pragma once

#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {

class TThrottler {
public:
    TThrottler() = default;

    TThrottler(TControlWrapper maxRatePerMinute, TControlWrapper maxBurst,
            TIntrusivePtr<ITimeProvider> timeProvider)
        : TimeProvider(std::move(timeProvider))
        , MaxRatePerMinute(std::move(maxRatePerMinute))
        , MaxBurst(std::move(maxBurst))
        , LastUpdate(TimeProvider->Now())
    {}

    bool Throttle() {
        auto maxRatePerMinute = static_cast<i64>(MaxRatePerMinute);
        auto maxBurst = static_cast<i64>(MaxBurst);
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
        i64 decrease = (now - LastUpdate) / deltaBetweenSends;
        decrease = std::min(decrease, CurrentBurst);
        Y_ABORT_UNLESS(decrease >= 0);
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
    i64 CurrentBurst = 0;
};

} // namespace NKikimr
