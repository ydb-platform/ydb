#include "monotonic.h"

#include <chrono>
#include <optional>
#include <system_error>
#include <util/system/platform.h>

#ifdef _linux_
    #include <time.h>
    #include <string.h>
#endif

namespace NMonotonic {

    namespace {
#if defined(_linux_) && defined(CLOCK_BOOTTIME)
        std::optional<ui64> GetClockBootTimeMicroSeconds() {
            struct timespec t;
            std::optional<ui64> r;
            if (0 == ::clock_gettime(CLOCK_BOOTTIME, &t)) {
                r.emplace(t.tv_nsec / 1000ULL + t.tv_sec * 1000000ULL);
            }
            return r;
        }
#endif

        struct TMonotonicSupport {
#if defined(_linux_) && defined(CLOCK_BOOTTIME)
            // We remember initial offset to measure time relative to program
            // start and so we never return a zero time.
            std::optional<ui64> BootTimeOffset;
#endif
            // Unfortunately time_since_epoch() is sometimes negative on wine
            // Remember initial time point at program start and use offsets from that
            std::chrono::steady_clock::time_point SteadyClockOffset;

            TMonotonicSupport() {
#if defined(_linux_) && defined(CLOCK_BOOTTIME)
                BootTimeOffset = GetClockBootTimeMicroSeconds();
#endif
                SteadyClockOffset = std::chrono::steady_clock::now();
            }

            ui64 GetMicroSeconds() const {
#if defined(_linux_) && defined(CLOCK_BOOTTIME)
                if (Y_LIKELY(BootTimeOffset)) {
                    auto r = GetClockBootTimeMicroSeconds();
                    if (Y_UNLIKELY(!r)) {
                        throw std::system_error(
                            std::error_code(errno, std::system_category()),
                            "clock_gettime(CLOCK_BOOTTIME) failed");
                    }
                    // Note: we add 1 so we never return zero
                    return *r - *BootTimeOffset + 1;
                }
#endif
                auto elapsed = std::chrono::steady_clock::now() - SteadyClockOffset;
                auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
                // Steady clock is supposed to never jump backwards, but it's
                // better to be safe in case of buggy implementations
                if (Y_UNLIKELY(microseconds < 0)) {
                    microseconds = 0;
                }
                // Note: we add 1 so we never return zero
                return ui64(microseconds) + 1;
            }
        };

        TMonotonicSupport MonotonicSupport;
    }

    ui64 GetMonotonicMicroSeconds() {
        return MonotonicSupport.GetMicroSeconds();
    }

}

template <>
void Out<NMonotonic::TMonotonic>(
    IOutputStream& o,
    NMonotonic::TMonotonic t)
{
    o << t - NMonotonic::TMonotonic::Zero();
}
