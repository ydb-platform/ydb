#pragma once

#include <library/cpp/time_provider/time_provider.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <utility>

namespace NKikimr {

// Starts full. Negative balance is red, and zero or positive balance is green.
// Balance saturates at the limits of i64.
class TLockFreeLightBucket {
public:
    TLockFreeLightBucket(ui64 capacityLimit, ui64 volumePerSecond, TIntrusivePtr<ITimeProvider> timeProvider = {});

    // Each call credits whole tokens earned since the last update. On platforms
    // without lock-free paired updates, a concurrent balance write can supersede this one.
    void Take(ui64 volume);
    // Cumulative {green, red} including idle time through this poll. An overlapping
    // writer can finish adding its interval after the counters have been sampled.
    std::pair<ui64, ui64> OverflowStats();
    void SetReplenishmentRate(ui64 volumePerSecond);
    void SetCapacity(ui64 capacityLimit);

private:
    struct alignas(16) TState {
        i64 Balance;
        ui64 TimestampNs;
    };

    ui64 NowNs() const;

    const THPTimer Timer;
    const TIntrusivePtr<ITimeProvider> TimeProvider;
    std::atomic<i64> Capacity;
    std::atomic<ui64> Rate;
    TState State;
    std::atomic<ui64> GreenNs{0};
    std::atomic<ui64> RedNs{0};
};

} // namespace NKikimr
