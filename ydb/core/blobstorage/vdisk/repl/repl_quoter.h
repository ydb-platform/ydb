#pragma once

namespace NKikimr {

    class TReplQuoter {
    public:
        using TPtr = std::shared_ptr<TReplQuoter>;

    private:
        using TAtomicInstant = std::atomic<TInstant>;
        static_assert(TAtomicInstant::is_always_lock_free);

        TAtomicInstant NextQueueItemTimestamp;
        const ui64 BytesPerSecond;

    public:
        TReplQuoter(ui64 bytesPerSecond)
            : BytesPerSecond(bytesPerSecond)
        {
            NextQueueItemTimestamp = TInstant::Zero();
        }

        TDuration Take(TInstant now, ui64 bytes) {
            TDuration duration = TDuration::MicroSeconds(bytes * 1000000 / BytesPerSecond);
            for (;;) {
                TInstant current = NextQueueItemTimestamp;
                const TInstant notBefore = now - GetCapacity();
                const TInstant base = Max(current, notBefore);
                const TDuration res = base - now; // time to wait until submitting desired query
                const TInstant next = base + duration;
                if (NextQueueItemTimestamp.compare_exchange_weak(current, next)) {
                    return res;
                }
            }
        }

        static void QuoteMessage(const TPtr& quoter, std::unique_ptr<IEventHandle> ev, ui64 bytes) {
            const TDuration timeout = quoter
                ? quoter->Take(TActivationContext::Now(), bytes)
                : TDuration::Zero();
            if (timeout != TDuration::Zero()) {
                TActivationContext::Schedule(timeout, ev.release());
            } else {
                TActivationContext::Send(ev.release());
            }
        }

        ui64 GetMaxPacketSize() const {
            return BytesPerSecond * GetCapacity().MicroSeconds() / 1000000;
        }

        static constexpr TDuration GetCapacity() {
            return TDuration::MilliSeconds(100);
        }
    };

} // NKikimr
