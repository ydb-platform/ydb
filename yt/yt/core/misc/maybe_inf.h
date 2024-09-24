#pragma once

#include <library/cpp/yt/string/format.h>
#include <compare>
#include <concepts>
#include <limits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <std::unsigned_integral T>
struct TMaybeInfTraits
{
    static constexpr T Infinity = std::numeric_limits<T>::max();
};

//! Represents possibly infinite unsigned integer.
/*!
 * This class represents possible infinite integer with some basic arithmetic.
 * The main usage of it is transferring quotas which can be infinite.
 */
template <std::unsigned_integral T>
class TMaybeInf
{
public:
    using TTraits = TMaybeInfTraits<T>;

    TMaybeInf() noexcept = default;

    TMaybeInf(const TMaybeInf&) noexcept = default;
    TMaybeInf& operator=(const TMaybeInf&) noexcept = default;

    template <std::unsigned_integral U>
    explicit(std::numeric_limits<U>::max() > TTraits::Infinity)
    TMaybeInf(TMaybeInf<U> that) noexcept;

    template <std::integral U>
    explicit(std::is_signed_v<U>)
    TMaybeInf(U value) noexcept;

    static TMaybeInf Infinity() noexcept;

    // Requires underlying value to be finite.
    T ToUnderlying() const noexcept;

    bool IsInfinite() const noexcept;
    bool IsFinite() const noexcept;

    //! Returns `false` on overflow or infinity-related operation.
    bool CanIncrease(TMaybeInf delta) const noexcept;
    bool CanDecrease(TMaybeInf delta) const noexcept;

    void Increase(T delta) noexcept;
    void Decrease(T delta) noexcept;

    void IncreaseWithInfinityAllowed(TMaybeInf that) noexcept;

    // NB: It works since `InfiniteValue` is greater than all other possible values.
    friend std::strong_ordering operator<=>(TMaybeInf lhs, TMaybeInf rhs) noexcept = default;

    // Needed for Save()/Load().
    T UnsafeToUnderlying() const noexcept;
    void UnsafeAssign(T value) noexcept;

protected:
    T Value_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, TMaybeInf<T> value, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MAYBE_INF_INL_H_
#include "maybe_inf-inl.h"
#undef MAYBE_INF_INL_H_
