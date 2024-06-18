#ifndef MAYBE_INF_INL_H_
#error "Direct inclusion of this file is not allowed, include maybe_inf.h"
// For the sake of sane code completion.
#include "maybe_inf.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <std::unsigned_integral T>
template <std::unsigned_integral U>
TMaybeInf<T>::TMaybeInf(TMaybeInf<U> that) noexcept
    : Value_(that.Value_)
{
    YT_ASSERT(that.Value_ < TTraits::Infinity);
}

template <std::unsigned_integral T>
template <std::integral U>
TMaybeInf<T>::TMaybeInf(U value) noexcept
    : Value_(static_cast<std::make_unsigned_t<U>>(value))
{
    if constexpr (std::is_signed_v<U>) {
        YT_ASSERT(value >= 0);
    }
    YT_ASSERT(static_cast<std::make_unsigned_t<U>>(value) < TTraits::Infinity);
}

template <std::unsigned_integral T>
TMaybeInf<T> TMaybeInf<T>::Infinity() noexcept
{
    // NB: Public constructor does not accept infinity.
    TMaybeInf result;
    result.Value_ = TTraits::Infinity;
    return result;
}

template <std::unsigned_integral T>
T TMaybeInf<T>::ToUnderlying() const noexcept
{
    YT_ASSERT(IsFinite());
    return Value_;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::IsInfinite() const noexcept
{
    return Value_ == TTraits::Infinity;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::IsFinite() const noexcept
{
    return !IsInfinite();
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::CanIncrease(TMaybeInf delta) const noexcept
{
    return
        delta.IsFinite() &&
        (IsFinite() || delta.Value_ == 0) &&
        Value_ + delta.Value_ >= Value_ &&
        Value_ + delta.Value_ < TTraits::Infinity;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::CanDecrease(TMaybeInf delta) const noexcept
{
    return
        delta.IsFinite() &&
        (IsFinite() || delta.Value_ == 0) &&
        Value_ >= delta.Value_;
}

template <std::unsigned_integral T>
void TMaybeInf<T>::Increase(T delta) noexcept
{
    YT_ASSERT(CanIncrease(delta));
    Value_ += delta;
}

template <std::unsigned_integral T>
void TMaybeInf<T>::Decrease(T delta) noexcept
{
    YT_ASSERT(CanDecrease(delta));
    Value_ -= delta;
}

template <std::unsigned_integral T>
void TMaybeInf<T>::IncreaseWithInfinityAllowed(TMaybeInf that) noexcept
{
    YT_ASSERT(IsInfinite() || that.IsInfinite() || (Value_ + that.Value_ >= Value_));
    if (IsInfinite() || that.IsInfinite()) [[unlikely]] {
        Value_ = TTraits::Infinity;
    } else {
        Value_ += that.Value_;
    }
}

template <std::unsigned_integral T>
void TMaybeInf<T>::UnsafeAssign(T value) noexcept
{
    Value_ = value;
}

template <std::unsigned_integral T>
T TMaybeInf<T>::UnsafeToUnderlying() const noexcept
{
    return Value_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, TMaybeInf<T> value, TStringBuf spec)
{
    if (value.IsFinite()) {
        FormatValue(builder, value.ToUnderlying(), spec);
    } else {
        FormatValue(builder, "inf", spec);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
