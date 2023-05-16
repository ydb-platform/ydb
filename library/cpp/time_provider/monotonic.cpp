#include "monotonic.h"

#include <chrono>
#include <optional>
#include <util/system/platform.h>

#ifdef _linux
    #include <time.h>
    #include <string.h>
#endif

namespace NMonotonic {

    namespace {
        // Unfortunately time_since_epoch() is sometimes negative on wine
        // Remember initial time point at program start and use offsets from that
        std::chrono::steady_clock::time_point MonotonicOffset = std::chrono::steady_clock::now();
    }

    ui64 GetMonotonicMicroSeconds() {
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - MonotonicOffset).count();
        // Steady clock is supposed to never jump backwards, but it's better to be safe in case of buggy implementations
        if (Y_UNLIKELY(microseconds < 0)) {
            microseconds = 0;
        }
        // Add one so we never return zero
        return microseconds + 1;
    }

#ifdef _linux_
    namespace {
        std::optional<ui64> GetClockBootTimeMicroSeconds() {
            struct timespec t;
            std::optional<ui64> r;
            if (0 == ::clock_gettime(CLOCK_BOOTTIME, &t)) {
                r.emplace(t.tv_nsec / 1000ULL + t.tv_sec * 1000000ULL);
            }
            return r;
        }

        // We want time relative to process start
        std::optional<ui64> BootTimeOffset = GetClockBootTimeMicroSeconds();
    }

    ui64 GetBootTimeMicroSeconds() {
        if (Y_UNLIKELY(!BootTimeOffset)) {
            return GetMonotonicMicroSeconds();
        }

        auto r = GetClockBootTimeMicroSeconds();
        Y_VERIFY(r, "Unexpected clock_gettime(CLOCK_BOOTTIME) failure: %s", strerror(errno));
        return *r - *BootTimeOffset + 1;
    }
#else
    ui64 GetBootTimeMicroSeconds() {
        return GetMonotonicMicroSeconds();
    }
#endif

}

template <>
void Out<NMonotonic::TMonotonic>(
    IOutputStream& o,
    NMonotonic::TMonotonic t)
{
    o << t - NMonotonic::TMonotonic::Zero();
}

template <>
void Out<NMonotonic::TBootTime>(
    IOutputStream& o,
    NMonotonic::TBootTime t)
{
    o << t - NMonotonic::TBootTime::Zero();
}
