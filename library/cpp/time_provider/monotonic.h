#pragma once

#include <util/datetime/base.h>

namespace NMonotonic {

    /**
     * Returns current monotonic time in microseconds
     *
     * On Linux uses CLOCK_BOOTTIME under the hood, so it includes time passed
     * during suspend and makes it safe for measuring lease times.
     */
    ui64 GetMonotonicMicroSeconds();

    /**
     * Similar to TInstant, but measuring monotonic time
     *
     * On Linux uses CLOCK_BOOTTIME under the hood, so it includes time passed
     * during suspend and makes it safe for measuring lease times.
     */
    class TMonotonic: public TTimeBase<TMonotonic> {
        using TBase = TTimeBase<TMonotonic>;

    protected:
        constexpr explicit TMonotonic(TValue value) noexcept
            : TBase(value)
        {
        }

    public:
        constexpr TMonotonic() noexcept {
        }

        static constexpr TMonotonic FromValue(TValue value) noexcept {
            return TMonotonic(value);
        }

        static inline TMonotonic Now() {
            return TMonotonic::MicroSeconds(GetMonotonicMicroSeconds());
        }

        using TBase::Days;
        using TBase::Hours;
        using TBase::MicroSeconds;
        using TBase::MilliSeconds;
        using TBase::Minutes;
        using TBase::Seconds;

        static constexpr TMonotonic Max() noexcept {
            return TMonotonic::FromValue(::Max<ui64>());
        }

        static constexpr TMonotonic Zero() noexcept {
            return TMonotonic::FromValue(0);
        }

        static constexpr TMonotonic MicroSeconds(ui64 us) noexcept {
            return TMonotonic::FromValue(TInstant::MicroSeconds(us).GetValue());
        }

        static constexpr TMonotonic MilliSeconds(ui64 ms) noexcept {
            return TMonotonic::FromValue(TInstant::MilliSeconds(ms).GetValue());
        }

        static constexpr TMonotonic Seconds(ui64 s) noexcept {
            return TMonotonic::FromValue(TInstant::Seconds(s).GetValue());
        }

        static constexpr TMonotonic Minutes(ui64 m) noexcept {
            return TMonotonic::FromValue(TInstant::Minutes(m).GetValue());
        }

        static constexpr TMonotonic Hours(ui64 h) noexcept {
            return TMonotonic::FromValue(TInstant::Hours(h).GetValue());
        }

        static constexpr TMonotonic Days(ui64 d) noexcept {
            return TMonotonic::FromValue(TInstant::Days(d).GetValue());
        }

        template <class T>
        inline TMonotonic& operator+=(const T& t) noexcept {
            return (*this = (*this + t));
        }

        template <class T>
        inline TMonotonic& operator-=(const T& t) noexcept {
            return (*this = (*this - t));
        }
    };

} // namespace NMonotonic

Y_DECLARE_PODTYPE(NMonotonic::TMonotonic);

namespace std {
    template <>
    struct hash<NMonotonic::TMonotonic> {
        size_t operator()(const NMonotonic::TMonotonic& key) const noexcept {
            return hash<NMonotonic::TMonotonic::TValue>()(key.GetValue());
        }
    };
}

namespace NMonotonic {

    constexpr TDuration operator-(const TMonotonic& l, const TMonotonic& r) {
        return TInstant::FromValue(l.GetValue()) - TInstant::FromValue(r.GetValue());
    }

    constexpr TMonotonic operator+(const TMonotonic& l, const TDuration& r) {
        TInstant result = TInstant::FromValue(l.GetValue()) + r;
        return TMonotonic::FromValue(result.GetValue());
    }

    constexpr TMonotonic operator-(const TMonotonic& l, const TDuration& r) {
        TInstant result = TInstant::FromValue(l.GetValue()) - r;
        return TMonotonic::FromValue(result.GetValue());
    }

} // namespace NMonotonic

// TODO: remove, alias for compatibility
using TMonotonic = NMonotonic::TMonotonic;
