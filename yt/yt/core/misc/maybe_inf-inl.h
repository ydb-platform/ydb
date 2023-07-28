#ifndef MAYBE_INF_INL_H_
#error "Direct inclusion of this file is not allowed, include maybe_inf.h"
// For the sake of sane code completion.
#include "maybe_inf.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <std::unsigned_integral T>
TMaybeInf<T>::TMaybeInf(T value) noexcept
    : Value_(value)
{
    YT_ASSERT(value != TTraits::InfiniteValue);
}

template <std::unsigned_integral T>
TMaybeInf<T> TMaybeInf<T>::Infinity() noexcept
{
    // NB: Avoid using public constructor which prohibits infinity.
    TMaybeInf result;
    result.Value_ = TTraits::InfiniteValue;
    return result;
}

template <std::unsigned_integral T>
T TMaybeInf<T>::ToUnderlying() const noexcept
{
    YT_ASSERT(!IsInfinity());
    return Value_;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::IsInfinity() const noexcept
{
    return Value_ == TTraits::InfiniteValue;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::CanBeIncreased(TMaybeInf delta) const noexcept
{
    return (!IsInfinity() && !delta.IsInfinity()) || Value_ == 0 || delta.Value_ == 0;
}

template <std::unsigned_integral T>
bool TMaybeInf<T>::CanBeDecreased(TMaybeInf delta) const noexcept
{
    return
        !delta.IsInfinity() &&
        (IsInfinity() ? delta.Value_ == 0 : Value_ >= delta.Value_);
}

template <std::unsigned_integral T>
void TMaybeInf<T>::IncreaseBy(TMaybeInf delta) noexcept
{
    YT_ASSERT(CanBeIncreased(delta));

    if (IsInfinity() || delta.IsInfinity()) {
        Value_ = TTraits::InfiniteValue;
    } else {
        auto newValue = Value_ + delta.ToUnderlying();
        // Check overflow.
        YT_ASSERT(newValue != TTraits::InfiniteValue);
        YT_ASSERT(newValue >= Value_);
        Value_ = newValue;
    }
}

template <std::unsigned_integral T>
void TMaybeInf<T>::DecreaseBy(TMaybeInf delta) noexcept
{
    YT_ASSERT(CanBeDecreased(delta));

    Value_ -= delta.Value_;
}

template <std::unsigned_integral T>
std::strong_ordering TMaybeInf<T>::operator<=>(TMaybeInf that) const noexcept
{
    // It works because infinity is represented as `numeric_limits<T>::max()`.
    return Value_ <=> that.Value_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
