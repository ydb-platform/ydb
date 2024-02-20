#pragma once

#include <algorithm>

#include <util/system/defaults.h>
#include <util/system/hp_timer.h>
#include <util/system/platform.h>

#if defined(_win_)
#include <intrin.h>
#pragma intrinsic(__rdtsc)
#endif // _win_

#if defined(_darwin_) && !defined(_x86_)
#include <mach/mach_time.h>
#endif

// GetCycleCount() from util/system/datetime.h uses rdtscp, which is more accurate than rdtsc,
// but rdtscp disables processor's out-of-order execution, so it can be slow
Y_FORCE_INLINE ui64 GetCycleCountFast() {
#if defined(_MSC_VER)
    // Generates the rdtsc instruction, which returns the processor time stamp.
    // The processor time stamp records the number of clock cycles since the last reset.
    return __rdtsc();
#elif defined(__clang__) && !defined(_arm64_)
    return __builtin_readcyclecounter();
#elif defined(_x86_64_)
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc"
                         : "=a"(lo), "=d"(hi));
    return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
#elif defined(_i386_)
    ui64 x;
    __asm__ volatile("rdtsc\n\t"
                     : "=A"(x));
    return x;
#elif defined(_darwin_)
    return mach_absolute_time();
#elif defined(_arm32_)
    return MicroSeconds();
#elif defined(_arm64_)
    ui64 x;

    __asm__ __volatile__("isb; mrs %0, cntvct_el0"
                         : "=r"(x));

    return x;
#else
#error "unsupported arch"
#endif
}

// NHPTimer::GetTime fast analog
Y_FORCE_INLINE void GetTimeFast(NHPTimer::STime* pTime) noexcept {
    *pTime = GetCycleCountFast();
}

namespace NActors {
    inline double Ts2Ns(ui64 ts) {
        return NHPTimer::GetSeconds(ts) * 1e9;
    }

    inline double Ts2Us(ui64 ts) {
        return NHPTimer::GetSeconds(ts) * 1e6;
    }

    inline double Ts2Ms(ui64 ts) {
        return NHPTimer::GetSeconds(ts) * 1e3;
    }

    inline ui64 Us2Ts(double us) {
        return ui64(NHPTimer::GetClockRate() * us / 1e6);
    }

    struct TTimeTracker {
        ui64 Ts;
        TTimeTracker(): Ts(GetCycleCountFast()) {}
        ui64 Elapsed() {
            ui64 ts = GetCycleCountFast();
            std::swap(Ts, ts);
            return Ts - ts;
        }
    };
}
