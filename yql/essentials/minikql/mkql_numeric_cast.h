#pragma once

#include <yql/essentials/public/decimal/yql_wide_int.h>

#include <type_traits>

namespace NKikimr {
namespace NMiniKQL {

// TMakeUnsigned - a safe alternative to std::make_unsigned that avoids UB
// when specializing for custom types like NYql::TWide
template <typename T>
struct TMakeUnsigned {
    using type = std::make_unsigned_t<T>;
};

template <typename T>
using TMakeUnsigned_t = typename TMakeUnsigned<T>::type;

// Specializations for NYql::TWide types
template <>
struct TMakeUnsigned<NYql::TWide<i8>> {
    using type = NYql::TWide<ui8>;
};

template <>
struct TMakeUnsigned<NYql::TWide<ui8>> {
    using type = NYql::TWide<ui8>;
};

template <>
struct TMakeUnsigned<NYql::TWide<i16>> {
    using type = NYql::TWide<ui16>;
};

template <>
struct TMakeUnsigned<NYql::TWide<ui16>> {
    using type = NYql::TWide<ui16>;
};

template <>
struct TMakeUnsigned<NYql::TWide<i32>> {
    using type = NYql::TWide<ui32>;
};

template <>
struct TMakeUnsigned<NYql::TWide<ui32>> {
    using type = NYql::TWide<ui32>;
};

template <>
struct TMakeUnsigned<NYql::TWide<i64>> {
    using type = NYql::TWide<ui64>;
};

template <>
struct TMakeUnsigned<NYql::TWide<ui64>> {
    using type = NYql::TWide<ui64>;
};

} // namespace NMiniKQL
} // namespace NKikimr
