#pragma once

#include <limits>
#include <type_traits>

namespace NKikimr {

    /**
     * Type safe alias for a primitive type, with transparent basic operations
     */
    template<class TDerived, class TValue>
    class TTypeSafeAlias {
        static_assert(std::is_arithmetic<TValue>::value, "TTypeSafeAlias is used for arithmetic types");

    protected:
        typedef TTypeSafeAlias<TDerived, TValue> TBase;
        typedef TDerived TSelf;

    protected:
        explicit constexpr TTypeSafeAlias(TValue value) noexcept
            : Value(value)
        { }

    public:
        static constexpr TDerived Zero() noexcept { return TDerived(TValue()); }
        static constexpr TDerived Min() noexcept { return TDerived(std::numeric_limits<TValue>::lowest()); }
        static constexpr TDerived Max() noexcept { return TDerived(std::numeric_limits<TValue>::max()); }

        constexpr bool operator<(const TBase& rhs) const noexcept { return Value < rhs.Value; }
        constexpr bool operator>(const TBase& rhs) const noexcept { return Value > rhs.Value; }
        constexpr bool operator<=(const TBase& rhs) const noexcept { return Value <= rhs.Value; }
        constexpr bool operator>=(const TBase& rhs) const noexcept { return Value >= rhs.Value; }
        constexpr bool operator==(const TBase& rhs) const noexcept { return Value == rhs.Value; }
        constexpr bool operator!=(const TBase& rhs) const noexcept { return Value != rhs.Value; }

        constexpr TDerived& operator++() noexcept { ++Value; return static_cast<TDerived&>(*this); }
        constexpr TDerived& operator--() noexcept { --Value; return static_cast<TDerived&>(*this); }
        constexpr TDerived operator++(int) noexcept { TDerived copy = *this; ++Value; return copy; }
        constexpr TDerived operator--(int) noexcept { TDerived copy = *this; --Value; return copy; }

        constexpr TDerived& operator+=(TValue value) noexcept { Value += value; return static_cast<TDerived&>(*this); }
        constexpr TDerived& operator-=(TValue value) noexcept { Value -= value; return static_cast<TDerived&>(*this); }
        constexpr TDerived& operator+=(const TBase& rhs) noexcept { Value += rhs.Value; return static_cast<TDerived&>(*this); }
        constexpr TDerived& operator-=(const TBase& rhs) noexcept { Value -= rhs.Value; return static_cast<TDerived&>(*this); }

        constexpr TDerived operator+(TValue value) const noexcept { return TDerived(Value + value); }
        constexpr TDerived operator-(TValue value) const noexcept { return TDerived(Value - value); }
        constexpr TDerived operator+(const TBase& rhs) const noexcept { return TDerived(Value + rhs.Value); }
        constexpr TDerived operator-(const TBase& rhs) const noexcept { return TDerived(Value - rhs.Value); }

    protected:
        TValue Value;
    };

} // namespace NKikimr
