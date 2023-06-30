#pragma once

#include <compare>
#include <concepts>
#include <limits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <std::unsigned_integral T>
struct TMaybeInfTraits
{
    constexpr static T InfiniteValue = std::numeric_limits<T>::max();
};

template <std::unsigned_integral T>
class TMaybeInf
{
public:
    using TTraits = TMaybeInfTraits<T>;

    TMaybeInf() noexcept = default;

    explicit TMaybeInf(T value) noexcept;
    TMaybeInf(const TMaybeInf&) noexcept = default;
    TMaybeInf& operator=(const TMaybeInf&) noexcept = default;

    static TMaybeInf Infinity() noexcept;

    T ToUnderlying() const noexcept;

    bool IsInfinity() const noexcept;

    bool CanBeIncreased(TMaybeInf delta) const noexcept;
    bool CanBeDecreased(TMaybeInf delta) const noexcept;

    void IncreaseBy(TMaybeInf delta) noexcept;
    void DecreaseBy(TMaybeInf delta) noexcept;

    std::strong_ordering operator<=>(TMaybeInf that) const noexcept;

protected:
    T Value_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MAYBE_INF_INL_H_
#include "maybe_inf-inl.h"
#undef MAYBE_INF_INL_H_
