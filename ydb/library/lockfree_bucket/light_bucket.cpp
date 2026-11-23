#include "light_bucket.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace NKikimr {
namespace {

constexpr ui64 NsPerSecond = 1'000'000'000;

static_assert(std::atomic<ui64>::is_always_lock_free && std::atomic<i64>::is_always_lock_free);

struct TUpdate {
    i64 Balance;
    ui64 RedNs;
};

// At the upper bound double rounds UINT64_MAX up to 2^64. Clamp before conversion.
ui64 SaturateToUi64(double value) {
    if (value >= static_cast<double>(std::numeric_limits<ui64>::max())) {
        return std::numeric_limits<ui64>::max();
    }
    return static_cast<ui64>(value);
}

TUpdate Calculate(i64 previousBalance, ui64 elapsedNs, double rate, i64 capacity, ui64 volume,
        TLockFreeLightBucket::EDebtLimit debtLimit) {
    i64 minBalance = std::numeric_limits<i64>::min();
    if (debtLimit == TLockFreeLightBucket::EDebtLimit::TwiceCapacity && capacity <= std::numeric_limits<i64>::max() / 2) {
        minBalance = -2 * capacity;
    }
    // A lower capacity can forgive some of the debt at the start of this interval.
    previousBalance = std::max(previousBalance, minBalance);
    const ui64 refill = SaturateToUi64(static_cast<double>(elapsedNs) * rate / NsPerSecond);

    i64 replenishedBalance;
    if (__builtin_add_overflow(previousBalance, refill, &replenishedBalance) || replenishedBalance > capacity) {
        replenishedBalance = capacity;
    }

    // Only refill acts during the elapsed interval. The withdrawal is instantaneous at its end.
    ui64 redNs = 0;
    if (replenishedBalance < 0) {
        redNs = elapsedNs;
    } else if (previousBalance < 0) {
        // Refill reached zero, so the rate is positive.
        const double recoveryNs = -static_cast<double>(previousBalance) * NsPerSecond / rate;
        redNs = std::min(elapsedNs, SaturateToUi64(std::ceil(recoveryNs)));
    }

    i64 nextBalance;
    if (__builtin_sub_overflow(replenishedBalance, volume, &nextBalance) || nextBalance < minBalance) {
        nextBalance = minBalance;
    }
    return {nextBalance, redNs};
}

} // namespace

ui64 TLockFreeLightBucket::TFullState::ElapsedTicks(ui64 now) const {
    return std::max(now, Timestamp) - Timestamp;
}

ui32 TLockFreeLightBucket::TCompactState::ElapsedTicks(ui32 now) const {
    // Unsigned subtraction handles a wrap. Within half a cycle, a larger delta is a stale reading.
    const ui32 elapsed = now - Timestamp;
    if (elapsed > std::numeric_limits<i32>::max()) {
        return 0;
    }
    return elapsed;
}

i64 TLockFreeLightBucket::CapacityLimit(ui64 capacity) {
    if constexpr (HasFullState) {
        return static_cast<i64>(std::min<ui64>(capacity, std::numeric_limits<i64>::max()));
    }
    return static_cast<i64>(std::min<ui64>(capacity / TCompactState::Scale, std::numeric_limits<i32>::max()));
}

ui64 TLockFreeLightBucket::NowNs() const {
    if (TimeProvider) {
        return TimeProvider->Now().NanoSeconds();
    }
    return SaturateToUi64(std::max(0.0, Timer.Passed()) * NsPerSecond);
}

TLockFreeLightBucket::TLockFreeLightBucket(ui64 capacityLimit, ui64 volumePerSecond,
        TIntrusivePtr<ITimeProvider> timeProvider, EDebtLimit debtLimit)
    : TimeProvider(std::move(timeProvider))
    , DebtLimit(debtLimit)
    , Capacity(CapacityLimit(capacityLimit))
    , Rate(volumePerSecond)
{
    if constexpr (HasFullState) {
        FullState = {CapacityLimit(capacityLimit), NowNs()};
    } else {
        CompactState = {static_cast<i32>(CapacityLimit(capacityLimit)), static_cast<ui32>(NowNs() / TCompactState::Scale)};
    }
}

template<class TState>
void TLockFreeLightBucket::Update(TState& storage, ui64 volume) {
    using TTimestamp = decltype(TState::Timestamp);
    using TBalance = decltype(TState::Balance);
    // Reading a new setting acquires the setter's preceding Take(0).
    // The balance CAS orders withdrawals by its own modification order. The
    // additive counters publish no other data and can be sampled independently.
    const ui64 withdrawal = volume / TState::Scale;
    const std::atomic_ref<TState> state(storage);
    TState previous = state.load(std::memory_order_acquire);
    TState next;
    TUpdate update;
    ui64 elapsedNs;
    do {
        const TTimestamp now = static_cast<TTimestamp>(NowNs() / TState::Scale);
        const TTimestamp elapsed = previous.ElapsedTicks(now);
        next.Timestamp = previous.Timestamp + elapsed;
        elapsedNs = static_cast<ui64>(elapsed) * TState::Scale;
        const double rate = static_cast<double>(Rate.load(std::memory_order_acquire)) / TState::Scale;
        update = Calculate(previous.Balance, elapsedNs, rate, Capacity.load(std::memory_order_acquire), withdrawal, DebtLimit);
        next.Balance = static_cast<TBalance>(std::max<i64>(update.Balance, std::numeric_limits<TBalance>::min()));
    } while (!state.compare_exchange_weak(previous, next, std::memory_order_release, std::memory_order_acquire));

    if (update.RedNs > 0) {
        RedNs.fetch_add(update.RedNs, std::memory_order_relaxed);
    }
    if (elapsedNs > update.RedNs) {
        GreenNs.fetch_add(elapsedNs - update.RedNs, std::memory_order_relaxed);
    }
}

void TLockFreeLightBucket::Take(ui64 volume) {
    if constexpr (HasFullState) {
        Update(FullState, volume);
    } else {
        Update(CompactState, volume);
    }
}

void TLockFreeLightBucket::Take(ui64 volume, ui64 capacityLimit) {
    if (CapacityLimit(capacityLimit) != Capacity.load(std::memory_order_acquire)) {
        SetCapacity(capacityLimit);
    }
    Take(volume);
}

std::pair<ui64, ui64> TLockFreeLightBucket::OverflowStats() {
    Take(0);
    return {GreenNs.load(std::memory_order_relaxed), RedNs.load(std::memory_order_relaxed)};
}

void TLockFreeLightBucket::SetReplenishmentRate(ui64 volumePerSecond) {
    Take(0);
    Rate.store(volumePerSecond, std::memory_order_release);
}

void TLockFreeLightBucket::SetCapacity(ui64 capacityLimit) {
    Take(0);
    Capacity.store(CapacityLimit(capacityLimit), std::memory_order_release);
}

} // namespace NKikimr
