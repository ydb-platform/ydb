#pragma once

#include <yql/essentials/minikql/mkql_numeric_cast.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <type_traits>
#include <concepts>

namespace NKikimr {
namespace NMiniKQL {

// Safe arithmetic operations that avoid undefined behavior from signed integer overflow
// by performing operations in unsigned arithmetic and casting back to signed.

// SafeAdd: Addition without signed overflow UB
template <typename T>
[[nodiscard]] constexpr T SafeAdd(T u, T v)
    requires std::is_integral_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>
{
    using TUnsigned = TMakeUnsigned_t<T>;
    return static_cast<T>(static_cast<TUnsigned>(u) +
                          static_cast<TUnsigned>(v));
}

template <typename T>
[[nodiscard]] constexpr T SafeAdd(T u, T v)
    requires std::is_floating_point_v<T>
{
    return u + v;
}

// SafeSub: Subtraction without signed overflow UB
template <typename T>
[[nodiscard]] constexpr T SafeSub(T u, T v)
    requires std::is_integral_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>
{
    using TUnsigned = TMakeUnsigned_t<T>;
    return static_cast<T>(static_cast<TUnsigned>(u) -
                          static_cast<TUnsigned>(v));
}

template <typename T>
[[nodiscard]] constexpr T SafeSub(T u, T v)
    requires std::is_floating_point_v<T>
{
    return u - v;
}

// SafeMul: Multiplication without signed overflow UB
// Special handling for 16-bit types to avoid implicit promotion overflow
template <typename T>
[[nodiscard]] constexpr T SafeMul(T u, T v)
    requires std::is_same_v<T, i16> || std::is_same_v<T, ui16>
{
    return static_cast<ui32>(u) * static_cast<ui32>(v);
}

template <typename T>
[[nodiscard]] constexpr T SafeMul(T u, T v)
    requires(std::is_integral_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>) &&
            (!std::is_same_v<T, i16>) && (!std::is_same_v<T, ui16>)
{
    using TUnsigned = TMakeUnsigned_t<T>;
    return static_cast<T>(static_cast<TUnsigned>(u) *
                          static_cast<TUnsigned>(v));
}

template <typename T>
[[nodiscard]] constexpr T SafeMul(T u, T v)
    requires std::is_floating_point_v<T>
{
    return u * v;
}

// SafeInc: Increment without signed overflow UB
template <typename T>
[[nodiscard]] constexpr T SafeInc(T u)
{
    return SafeAdd(u, T{1});
}

// SafeDec: Decrement without signed overflow UB
template <typename T>
[[nodiscard]] constexpr T SafeDec(T u)
{
    return SafeSub(u, T{1});
}

// SafeNeg: Negation without signed overflow UB (e.g., -INT_MIN)
// Uses two's complement: -x = ~x + 1
template <typename T>
[[nodiscard]] constexpr T SafeNeg(T u)
    requires std::is_integral_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>
{
    using TUnsigned = TMakeUnsigned_t<T>;
    return static_cast<T>(~static_cast<TUnsigned>(u) + TUnsigned{1});
}

template <typename T>
[[nodiscard]] constexpr T SafeNeg(T u)
    requires std::is_floating_point_v<T>
{
    return -u;
}

} // namespace NMiniKQL
} // namespace NKikimr
