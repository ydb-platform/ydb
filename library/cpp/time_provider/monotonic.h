#pragma once

#include <util/datetime/base.h>

namespace NMonotonic {

    template <class TDerived>
    class TMonotonicBase: public TTimeBase<TDerived> {
        using TBase = TTimeBase<TDerived>;

    public:
        using TBase::TBase;

        using TBase::Days;
        using TBase::Hours;
        using TBase::MicroSeconds;
        using TBase::MilliSeconds;
        using TBase::Minutes;
        using TBase::Seconds;

        static constexpr TDerived Max() noexcept {
            return TDerived::FromValue(::Max<ui64>());
        }

        static constexpr TDerived Zero() noexcept {
            return TDerived::FromValue(0);
        }

        static constexpr TDerived MicroSeconds(ui64 us) noexcept {
            return TDerived::FromValue(TInstant::MicroSeconds(us).GetValue());
        }

        static constexpr TDerived MilliSeconds(ui64 ms) noexcept {
            return TDerived::FromValue(TInstant::MilliSeconds(ms).GetValue());
        }

        static constexpr TDerived Seconds(ui64 s) noexcept {
            return TDerived::FromValue(TInstant::Seconds(s).GetValue());
        }

        static constexpr TDerived Minutes(ui64 m) noexcept {
            return TDerived::FromValue(TInstant::Minutes(m).GetValue());
        }

        static constexpr TDerived Hours(ui64 h) noexcept {
            return TDerived::FromValue(TInstant::Hours(h).GetValue());
        }

        static constexpr TDerived Days(ui64 d) noexcept {
            return TDerived::FromValue(TInstant::Days(d).GetValue());
        }
    };

    /**
     * Returns current monotonic time in microseconds
     */
    ui64 GetMonotonicMicroSeconds();

    /**
     * Similar to TInstant, but measuring monotonic time
     */
    class TMonotonic: public TMonotonicBase<TMonotonic> {
        using TBase = TMonotonicBase<TMonotonic>;

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

        template <class T>
        inline TMonotonic& operator+=(const T& t) noexcept {
            return (*this = (*this + t));
        }

        template <class T>
        inline TMonotonic& operator-=(const T& t) noexcept {
            return (*this = (*this - t));
        }
    };

    /**
     * Returns current CLOCK_BOOTTIME time in microseconds
     */
    ui64 GetBootTimeMicroSeconds();

    /**
     * Similar to TInstant, but measuring CLOCK_BOOTTIME time
     */
    class TBootTime: public TMonotonicBase<TBootTime> {
        using TBase = TMonotonicBase<TBootTime>;

    protected:
        constexpr explicit TBootTime(TValue value) noexcept
            : TBase(value)
        {
        }

    public:
        constexpr TBootTime() noexcept {
        }

        static constexpr TBootTime FromValue(TValue value) noexcept {
            return TBootTime(value);
        }

        static inline TBootTime Now() {
            return TBootTime::MicroSeconds(GetBootTimeMicroSeconds());
        }

        using TBase::Days;
        using TBase::Hours;
        using TBase::MicroSeconds;
        using TBase::MilliSeconds;
        using TBase::Minutes;
        using TBase::Seconds;

        template <class T>
        inline TBootTime& operator+=(const T& t) noexcept {
            return (*this = (*this + t));
        }

        template <class T>
        inline TBootTime& operator-=(const T& t) noexcept {
            return (*this = (*this - t));
        }
    };

} // namespace NMonotonic

Y_DECLARE_PODTYPE(NMonotonic::TMonotonic);
Y_DECLARE_PODTYPE(NMonotonic::TBootTime);

namespace std {
    template <>
    struct hash<NMonotonic::TMonotonic> {
        size_t operator()(const NMonotonic::TMonotonic& key) const noexcept {
            return hash<NMonotonic::TMonotonic::TValue>()(key.GetValue());
        }
    };

    template <>
    struct hash<NMonotonic::TBootTime> {
        size_t operator()(const NMonotonic::TBootTime& key) const noexcept {
            return hash<NMonotonic::TBootTime::TValue>()(key.GetValue());
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

    constexpr TDuration operator-(const TBootTime& l, const TBootTime& r) {
        return TInstant::FromValue(l.GetValue()) - TInstant::FromValue(r.GetValue());
    }

    constexpr TBootTime operator+(const TBootTime& l, const TDuration& r) {
        TInstant result = TInstant::FromValue(l.GetValue()) + r;
        return TBootTime::FromValue(result.GetValue());
    }

    constexpr TBootTime operator-(const TBootTime& l, const TDuration& r) {
        TInstant result = TInstant::FromValue(l.GetValue()) - r;
        return TBootTime::FromValue(result.GetValue());
    }

} // namespace NMonotonic

// TODO: remove, alias for compatibility
using TMonotonic = NMonotonic::TMonotonic;
