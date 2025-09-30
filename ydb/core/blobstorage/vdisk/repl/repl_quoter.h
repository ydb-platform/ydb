#pragma once

namespace NKikimr {

    class TReplQuoter {
    public:
        using TPtr = std::shared_ptr<TReplQuoter>;

    private:
        using TAtomicMonotonic = std::atomic<TMonotonic>;
        static_assert(TAtomicMonotonic::is_always_lock_free);

        TAtomicMonotonic NextQueueItemTimestamp;
        std::atomic_uint64_t BytesPerSecond;

    public:
        TReplQuoter(ui64 bytesPerSecond)
            : BytesPerSecond(bytesPerSecond)
        {
            NextQueueItemTimestamp = TMonotonic::Zero();
        }

        TDuration Take(TMonotonic now, ui64 bytes) {
            TDuration duration = TDuration::MicroSeconds(bytes * 1000000 / BytesPerSecond.load());
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

        void UpdateBytesPerSecond(ui64 bytesPerSecond) {
            BytesPerSecond.store(bytesPerSecond);
        }

        static void QuoteMessage(const TPtr& quoter, std::unique_ptr<IEventHandle> ev, ui64 bytes) {
            const TDuration timeout = quoter
                ? quoter->Take(TActivationContext::Monotonic(), bytes)
                : TDuration::Zero();
            if (timeout != TDuration::Zero()) {
                TActivationContext::Schedule(timeout, ev.release());
            } else {
                TActivationContext::Send(ev.release());
            }
        }

        ui64 GetMaxPacketSize() const {
            return BytesPerSecond.load() * GetCapacity().MicroSeconds() / 1000000;
        }

        static constexpr TDuration GetCapacity() {
            return TDuration::MilliSeconds(100);
        }
    };

} // NKikimr
