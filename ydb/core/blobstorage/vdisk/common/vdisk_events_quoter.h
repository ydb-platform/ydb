#pragma once

#include <functional>

namespace NKikimr {

    class TEventsQuoter {
    public:
        using TPtr = std::shared_ptr<TEventsQuoter>;

    private:
        using TAtomicMonotonic = std::atomic<TMonotonic>;
        static_assert(TAtomicMonotonic::is_always_lock_free);

        TAtomicMonotonic NextQueueItemTimestamp;
        // End of last throttled interval
        TAtomicMonotonic ThrottledUntilTimestamp;
        std::atomic_uint64_t DefaultBytesPerSecond = 0;

    public:
        TEventsQuoter() {
            NextQueueItemTimestamp = TMonotonic::Zero();
            ThrottledUntilTimestamp = TMonotonic::Zero();
        }

        TEventsQuoter(ui64 bytesPerSecond)
            : DefaultBytesPerSecond(bytesPerSecond)
        {
            NextQueueItemTimestamp = TMonotonic::Zero();
            ThrottledUntilTimestamp = TMonotonic::Zero();
        }

        TDuration Take(TMonotonic now, ui64 bytes, ui64 bytesPerSecond = 0) {
            auto bytesRate = GetBytesRate(bytesPerSecond);
            if (!bytesRate) {
                return TDuration::Zero();
            }
            TDuration duration = TDuration::MicroSeconds(bytes * 1000000 / bytesRate);
            for (;;) {
                TMonotonic current = NextQueueItemTimestamp;
                const TMonotonic notBefore = now - GetCapacity();
                const TMonotonic base = Max(current, notBefore);
                const TDuration res = base - now; // time to wait until submitting desired query
                const TMonotonic next = base + duration;
                if (NextQueueItemTimestamp.compare_exchange_weak(current, next)) {
                    return res;
                }
            }
        }

        void UpdateBytesPerSecond(ui64 defaultBytesPerSecond) {
            DefaultBytesPerSecond.store(defaultBytesPerSecond);
        }

        static void QuoteMessage(const TPtr& quoter, std::unique_ptr<IEventHandle> ev, ui64 bytes, ui64 bytesPerSecond = 0,
            ::NMonitoring::TDynamicCounters::TCounterPtr throttledCounter = {}) {
            const TMonotonic now = TActivationContext::Monotonic();
            const TDuration timeout = quoter
                ? quoter->Take(now, bytes, bytesPerSecond)
                : TDuration::Zero();
            if (timeout != TDuration::Zero()) {
                TActivationContext::Schedule(timeout, ev.release());
                const ui64 deltaMicrosec = quoter->MergeThrottledIntervalAndGetDeltaMicrosec(now, timeout);
                if (throttledCounter && deltaMicrosec) {
                    throttledCounter->Add(deltaMicrosec);
                }
            } else {
                TActivationContext::Send(ev.release());
            }
        }

        ui64 GetMaxPacketSize(ui64 bytesPerSecond = 0) const {
            auto bytesRate = GetBytesRate(bytesPerSecond);
            if (!bytesRate) {
                return std::numeric_limits<ui64>::max();
            }
            return bytesRate * GetCapacity().MicroSeconds() / 1000000;
        }

        static constexpr TDuration GetCapacity() {
            return TDuration::MilliSeconds(100);
        }

        ui64 MergeThrottledIntervalAndGetDeltaMicrosec(TMonotonic now, TDuration timeout) {
            for (;;) {
                TMonotonic currentThrottledUntil = ThrottledUntilTimestamp.load();
                const TMonotonic scheduledAt = now + timeout;
                const TMonotonic newThrottledUntil = Max(currentThrottledUntil, scheduledAt);
                if (ThrottledUntilTimestamp.compare_exchange_weak(currentThrottledUntil, newThrottledUntil)) {
                    const TMonotonic start = Max(now, currentThrottledUntil);
                    return (scheduledAt > start) ? (scheduledAt - start).MicroSeconds() : 0;
                }
            }
        }

    private:
        ui64 GetBytesRate(ui64 bytesPerSecond = 0) const {
            return bytesPerSecond ? bytesPerSecond : DefaultBytesPerSecond.load();
        }
    };

    using TReplQuoter = TEventsQuoter;

} // NKikimr
