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
    enum class EDebtLimit : ui8 {
        Unbounded = 0,
        TwiceCapacity,
    };

    TLockFreeLightBucket(ui64 capacityLimit, ui64 volumePerSecond, TIntrusivePtr<ITimeProvider> timeProvider = {},
        EDebtLimit debtLimit = EDebtLimit::Unbounded);

    // Each call credits whole tokens earned since the last update. On platforms
    // without lock-free paired updates, a concurrent balance write can supersede this one.
    void Take(ui64 volume);
    // Apply a live capacity change before charging the request.
    void Take(ui64 volume, ui64 capacityLimit);
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
    const EDebtLimit DebtLimit;
    std::atomic<i64> Capacity;
    std::atomic<ui64> Rate;
    TState State;
    std::atomic<ui64> GreenNs{0};
    std::atomic<ui64> RedNs{0};
};

} // namespace NKikimr
