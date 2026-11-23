#pragma once

#include <library/cpp/time_provider/time_provider.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <utility>

namespace NKikimr {

// Starts full. Negative balance is red, and zero or positive balance is green.
// With lock-free 128-bit CAS, the balance is i64 and timestamps are in nanoseconds.
// Otherwise the balance is i32 in units of 1,000 and timestamps are in microseconds.
// In that case, take or poll within 2^31 - 1 microseconds (~35m47s) of the previous update.
class TLockFreeLightBucket {
public:
    enum class EDebtLimit : ui8 {
        Unbounded = 0,
        TwiceCapacity,
    };

    TLockFreeLightBucket(ui64 capacityLimit, ui64 volumePerSecond, TIntrusivePtr<ITimeProvider> timeProvider = {},
        EDebtLimit debtLimit = EDebtLimit::Unbounded);

    // Refill and withdrawal are rounded down to whole balance units on each call.
    void Take(ui64 volume);
    // Apply a live capacity change before charging the request.
    void Take(ui64 volume, ui64 capacityLimit);
    // Cumulative {green, red} including idle time through this poll. An overlapping
    // writer can finish adding its interval after the counters have been sampled.
    std::pair<ui64, ui64> OverflowStats();
    void SetReplenishmentRate(ui64 volumePerSecond);
    void SetCapacity(ui64 capacityLimit);

private:
    struct alignas(16) TFullState {
        static constexpr ui64 Scale = 1;
        i64 Balance;
        ui64 Timestamp;
        ui64 ElapsedTicks(ui64 now) const;
    };

    struct alignas(8) TCompactState {
        static constexpr ui64 Scale = 1'000;
        i32 Balance;
        ui32 Timestamp;
        ui32 ElapsedTicks(ui32 now) const;
    };

    static_assert(sizeof(TFullState) == 2 * sizeof(ui64));
    static_assert(sizeof(TCompactState) == sizeof(ui64));
    static_assert(std::atomic_ref<TCompactState>::is_always_lock_free);
    static constexpr bool HasFullState = std::atomic_ref<TFullState>::is_always_lock_free;

    static i64 CapacityLimit(ui64 capacity);
    ui64 NowNs() const;
    template<class TState>
    void Update(TState& storage, ui64 volume);

    const THPTimer Timer;
    const TIntrusivePtr<ITimeProvider> TimeProvider;
    const EDebtLimit DebtLimit;
    std::atomic<i64> Capacity;
    std::atomic<ui64> Rate;
    std::atomic<ui64> GreenNs{0};
    std::atomic<ui64> RedNs{0};

    // Use the same cache-line alignment as TLockFreeBucket while keeping each CAS operand its native width.
    union alignas(128) {
        TFullState FullState;
        TCompactState CompactState;
    };
};

} // namespace NKikimr
