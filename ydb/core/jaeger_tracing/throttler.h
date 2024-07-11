#pragma once

#include <library/cpp/int128/int128.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NJaegerTracing {

class TThrottler: public TThrRefBase {
public:
    TThrottler(ui64 maxRatePerMinute, ui64 maxBurst, TIntrusivePtr<ITimeProvider> timeProvider);

    bool Throttle();

private:
    static ui64 ClampAdd(ui64 a, ui64 b);
    static ui64 ClampMultiply(ui64 a, ui64 b);

    const ui64 MaxTracesPerMinute;
    const ui64 MaxTracesBurst;
    const ui64 BetweenSends;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    std::atomic<ui64> EffectiveTs;

    static_assert(decltype(EffectiveTs)::is_always_lock_free);
};

} // namespace NKikimr::NJaegerTracing
