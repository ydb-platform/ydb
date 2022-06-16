#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

namespace NBus {
    /* Consumer and feeder quota model impl.

        Consumer thread only calls:
            Acquire(),  fetches tokens for usage from bucket;
            Consume(),  eats given amount of tokens, must not
                        be greater than Value() items;

        Other threads (feeders) calls:
            Return(),   put used tokens back to bucket;
     */

    class TTokenQuota {
    public:
        TTokenQuota(bool enabled, size_t tokens, size_t wake)
            : Enabled(tokens > 0 ? enabled : false)
            , Acquired(0)
            , WakeLev(wake < 1 ? Max<size_t>(1, tokens / 2) : 0)
            , Tokens_(tokens)
        {
            Y_UNUSED(padd_);
        }

        bool Acquire(TAtomicBase level = 1, bool force = false) {
            level = Max(TAtomicBase(level), TAtomicBase(1));

            if (Enabled && (Acquired < level || force)) {
                Acquired += AtomicSwap(&Tokens_, 0);
            }

            return !Enabled || Acquired >= level;
        }

        void Consume(size_t items) {
            if (Enabled) {
                Y_ASSERT(Acquired >= TAtomicBase(items));

                Acquired -= items;
            }
        }

        bool Return(size_t items_) noexcept {
            if (!Enabled || items_ == 0)
                return false;

            const TAtomic items = items_;
            const TAtomic value = AtomicAdd(Tokens_, items);

            return (value - items < WakeLev && value >= WakeLev);
        }

        bool IsEnabled() const noexcept {
            return Enabled;
        }

        bool IsAboveWake() const noexcept {
            return !Enabled || (WakeLev <= AtomicGet(Tokens_));
        }

        size_t Tokens() const noexcept {
            return Acquired + AtomicGet(Tokens_);
        }

        size_t Check(const TAtomicBase level) const noexcept {
            return !Enabled || level <= Acquired;
        }

    private:
        bool Enabled;
        TAtomicBase Acquired;
        const TAtomicBase WakeLev;
        TAtomic Tokens_;

        /* This padd requires for align Tokens_ member on its own
            CPU cacheline. */

        ui64 padd_;
    };
}
