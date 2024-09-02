#pragma once

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

struct TInstantTimerMs {
    using TTime = TInstant;
    static constexpr ui64 Resolution = 1000ull; // milliseconds
    static TTime Now() {
        return TInstant::Now();
    }
    static ui64 Duration(TTime from, TTime to) {
        return (to - from).MilliSeconds();
    }
};

struct THPTimerUs {
    using TTime = NHPTimer::STime;
    static constexpr ui64 Resolution = 1000000ull; // microseconds
    static TTime Now() {
        NHPTimer::STime ret;
        NHPTimer::GetTime(&ret);
        return ret;
    }
    static ui64 Duration(TTime from, TTime to) {
        i64 cycles = to - from;
        if (cycles > 0) {
            return ui64(double(cycles) * double(Resolution) / NHPTimer::GetClockRate());
        } else {
            return 0;
        }
    }
};
