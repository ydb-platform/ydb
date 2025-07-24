#pragma once

namespace NKikimr {

    class TEventsQuoter {
    public:
        using TPtr = std::shared_ptr<TEventsQuoter>;

    private:
        using TAtomicInstant = std::atomic<TInstant>;
        static_assert(TAtomicInstant::is_always_lock_free);

        TAtomicInstant NextQueueItemTimestamp;
        const ui64 DefaultBytesPerSecond = 0;

    public:
        TEventsQuoter() {
            NextQueueItemTimestamp = TInstant::Zero();
        }

        TEventsQuoter(ui64 bytesPerSecond)
            : DefaultBytesPerSecond(bytesPerSecond) 
        {
            NextQueueItemTimestamp = TInstant::Zero();
        }

        TDuration Take(TInstant now, ui64 bytes, ui64 bytesPerSecond = 0) {
            auto bytesRate = GetBytesRate(bytesPerSecond);
            if (!bytesRate) {
                return TDuration::Zero();
            }
            TDuration duration = TDuration::MicroSeconds(bytes * 1000000 / bytesRate);
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

        static void QuoteMessage(const TPtr& quoter, std::unique_ptr<IEventHandle> ev, ui64 bytes, ui64 bytesPerSecond = 0) {
            const TDuration timeout = quoter
                ? quoter->Take(TActivationContext::Now(), bytes, bytesPerSecond)
                : TDuration::Zero();
            if (timeout != TDuration::Zero()) {
                TActivationContext::Schedule(timeout, ev.release());
            } else {
                TActivationContext::Send(ev.release());
            }
        }

        ui64 GetMaxPacketSize(ui64 bytesPerSecond = 0) const {
            auto bytesRate = GetBytesRate(bytesPerSecond);
            if (!bytesRate) {
                std::numeric_limits<ui64>::max();
            }
            return bytesRate * GetCapacity().MicroSeconds() / 1000000;
        }

        static constexpr TDuration GetCapacity() {
            return TDuration::MilliSeconds(100);
        }

    private:
        ui64 GetBytesRate(ui64 bytesPerSecond = 0) const {
            return bytesPerSecond ? bytesPerSecond : DefaultBytesPerSecond;
        }
    };
    
    using TReplQuoter = TEventsQuoter;

} // NKikimr
