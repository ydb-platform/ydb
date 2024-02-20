#include "throttler.h"

#include <util/system/spinlock.h>

namespace NKikimr::NJaegerTracing {

TThrottler::TThrottler(ui64 maxRatePerMinute, ui64 maxBurst, TIntrusivePtr<ITimeProvider> timeProvider)
    : MaxRatePerMinute(maxRatePerMinute)
    , MaxBurst(maxBurst + 1)
    , BetweenSends(TDuration::Minutes(1).MicroSeconds() / MaxRatePerMinute)
    , TimeProvider(std::move(timeProvider))
    , EffectiveTs(TimeProvider->Now().MicroSeconds())
{}

bool TThrottler::Throttle() {
    auto now = TimeProvider->Now().MicroSeconds();
    auto ts = EffectiveTs.load(std::memory_order_relaxed);
    auto maxFinalTs = ClampAdd(now, ClampMultiply(BetweenSends, MaxBurst));
    while (true) {
        if (ts < now) {
            if (EffectiveTs.compare_exchange_weak(ts, now + BetweenSends, std::memory_order_relaxed)) {
                return false;
            }
            SpinLockPause();
        } else if (ts + BetweenSends > maxFinalTs) {
            return true;
        } else if (EffectiveTs.fetch_add(BetweenSends, std::memory_order_relaxed) + BetweenSends > maxFinalTs) {
            EffectiveTs.fetch_sub(BetweenSends, std::memory_order_relaxed);
            return true;
        } else {
            return false;
        }
    }
}

ui64 TThrottler::ClampAdd(ui64 a, ui64 b) {
    return
    #if defined(__has_builtin) && __has_builtin(__builtin_add_overflow)
    ClampAddBuiltin(a, b)
    #else
    ClampAddBackup(a, b)
    #endif
    ;
}

ui64 TThrottler::ClampMultiply(ui64 a, ui64 b) {
    return
    #if defined(__has_builtin) && __has_builtin(__builtin_mul_overflow)
    ClampMultiplyBuiltin(a, b)
    #else
    ClampMultiplyBackup(a, b)
    #endif
    ;
}

ui64 TThrottler::ClampAddBuiltin(ui64 a, ui64 b) {
    ui64 res;
    if (__builtin_add_overflow(a, b, &res)) {
        return Max<ui64>();
    } else {
        return res;
    }
}

ui64 TThrottler::ClampAddFallback(ui64 a, ui64 b) {
    if (a > Max<ui64>() - b) {
        return Max<ui64>();
    }
    return a + b;
}

ui64 TThrottler::ClampMultiplyBuiltin(ui64 a, ui64 b) {
    ui64 res;
    if (__builtin_mul_overflow(a, b, &res)) {
        return Max<ui64>();
    } else {
        return res;
    }
}

ui64 TThrottler::ClampMultiplyFallback(ui64 a, ui64 b) {
    ui128 prod = a;
    prod *= b;
    if (prod > Max<ui64>()) {
        return Max<ui64>();
    }
    return static_cast<ui64>(prod);
}

} // namespace NKikimr::NJaegerTracing
