#pragma once

#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {

class TThrottler {
public:
    TThrottler(TControlWrapper& maxRatePerMinute, TControlWrapper& maxBurst)
        : MaxRatePerMinute(maxRatePerMinute)
        , MaxBurst(maxBurst)
    {}

    bool Throttle() {
        auto maxRatePerMinute = MaxRatePerMinute;
        auto maxBurst = MaxBurst;
        auto maxTotal = maxRatePerMinute + maxBurst;
        if (maxRatePerMinute == 0) {
            CurrentBurst = 0;
            return true;
        }

        auto now = TAppData::TimeProvider->Now();
        if (now < LastUpdate) {
            return true;
        }

        const auto deltaBetweenSends = TDuration::Minutes(1) / MaxRatePerMinute;
        UpdateStats(now, deltaBetweenSends);

        if (CurrentBurst < maxTotal) {
            CurrentBurst += 1;
            return false;
        }

        return true;
    }

private:
    void UpdateStats(TInstant now, TDuration deltaBetweenSends) {
        if (now >= LastUpdate + deltaBetweenSends * CurrentBurst) {
            CurrentBurst = 0;
            LastUpdate = now;
        } else {
            i64 decrease = (now - LastUpdate) / deltaBetweenSends;
            Y_ABORT_UNLESS(decrease >= 0 && decrease < CurrentBurst);
            CurrentBurst -= decrease;
            LastUpdate += decrease * deltaBetweenSends;
        }
    }

    TControlWrapper& MaxRatePerMinute;
    TControlWrapper& MaxBurst;

    TInstant LastUpdate = TInstant::Zero();
    i64 CurrentBurst = 0;
};

} // namespace NKikimr
