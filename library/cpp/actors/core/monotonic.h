#pragma once 
 
#include <util/datetime/base.h> 
 
namespace NActors { 
 
    /** 
     * Returns current monotonic time in microseconds 
     */ 
    ui64 GetMonotonicMicroSeconds(); 
 
    /** 
     * Similar to TInstant, but measuring monotonic time 
     */ 
    class TMonotonic : public TTimeBase<TMonotonic> { 
        using TBase = TTimeBase<TMonotonic>; 
 
    private: 
        constexpr explicit TMonotonic(TValue value) noexcept 
            : TBase(value) 
        { } 
 
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
            return TMonotonic(::Max<ui64>()); 
        } 
 
        static constexpr TMonotonic Zero() noexcept { 
            return TMonotonic(); 
        } 
 
        static constexpr TMonotonic MicroSeconds(ui64 us) noexcept { 
            return TMonotonic(TInstant::MicroSeconds(us).GetValue()); 
        } 
 
        static constexpr TMonotonic MilliSeconds(ui64 ms) noexcept { 
            return TMonotonic(TInstant::MilliSeconds(ms).GetValue()); 
        } 
 
        static constexpr TMonotonic Seconds(ui64 s) noexcept { 
            return TMonotonic(TInstant::Seconds(s).GetValue()); 
        } 
 
        static constexpr TMonotonic Minutes(ui64 m) noexcept { 
            return TMonotonic(TInstant::Minutes(m).GetValue()); 
        } 
 
        static constexpr TMonotonic Hours(ui64 h) noexcept { 
            return TMonotonic(TInstant::Hours(h).GetValue()); 
        } 
 
        static constexpr TMonotonic Days(ui64 d) noexcept { 
            return TMonotonic(TInstant::Days(d).GetValue()); 
        } 
 
        template<class T> 
        inline TMonotonic& operator+=(const T& t) noexcept { 
            return (*this = (*this + t)); 
        } 
 
        template<class T> 
        inline TMonotonic& operator-=(const T& t) noexcept { 
            return (*this = (*this - t)); 
        } 
    }; 
} // namespace NActors 
 
Y_DECLARE_PODTYPE(NActors::TMonotonic); 
 
template<> 
struct THash<NActors::TMonotonic> { 
    size_t operator()(const NActors::TMonotonic& key) const { 
        return THash<NActors::TMonotonic::TValue>()(key.GetValue()); 
    } 
}; 
 
namespace NActors { 
 
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
 
} // namespace NActors 
