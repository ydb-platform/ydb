#pragma once

#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NJaegerTracing {

class TThrottler: public TThrRefBase {
public:
    TThrottler(ui64 maxRatePerMinute, ui64 maxBurst, TIntrusivePtr<ITimeProvider> timeProvider)
        : MaxRatePerMinute(maxRatePerMinute)
        , MaxBurst(maxBurst)
        , BetweenSends(TDuration::Minutes(1).MicroSeconds() / MaxRatePerMinute)
        , TimeProvider(std::move(timeProvider))
        , EffectiveTs(timeProvider->Now().MicroSeconds())
    {}

    bool Throttle() {
        auto now = TimeProvider->Now().MicroSeconds();
        auto ts = EffectiveTs.load(std::memory_order_relaxed);
        auto maxFinalTs = ClampAdd(now, ClampMultiply(BetweenSends, MaxBurst));
        while (true) {
            if (ts < now) {
                if (EffectiveTs.compare_exchange_weak(ts, now + BetweenSends, std::memory_order_relaxed)) {
                    return false;
                }
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

private:
    static ui64 ClampAdd(ui64 a, ui64 b) {
        ui64 res;
        if (__builtin_add_overflow(a, b, &res)) {
            return Max<ui64>();
        } else {
            return res;
        }
    }

    static ui64 ClampMultiply(ui64 a, ui64 b) {
        ui64 res;
        if (__builtin_mul_overflow(a, b, &res)) {
            return Max<ui64>();
        } else {
            return res;
        }
    }

    const ui64 MaxRatePerMinute;
    const ui64 MaxBurst;
    const ui64 BetweenSends;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    std::atomic<ui64> EffectiveTs;

    static_assert(decltype(EffectiveTs)::is_always_lock_free);
};

} // namespace NKikimr::NJaegerTracing
