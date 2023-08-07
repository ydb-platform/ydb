#pragma once

#include "fwd.h"

#include <tuple>
#include <type_traits>

namespace NYT {

/// @cond Doxygen_Suppress

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TDerived>
struct TIsBaseOf
{
    static constexpr bool Value = std::is_base_of_v<TBase, TDerived> && !std::is_same_v<TBase, TDerived>;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class Tuple>
struct TIndexInTuple;

template <class T, class... Types>
struct TIndexInTuple<T, std::tuple<T, Types...>>
{
    static constexpr int Value = 0;
};

template <class T>
struct TIndexInTuple<T, std::tuple<>>
{
    static constexpr int Value = 0;
};

template <class T, class U, class... Types>
struct TIndexInTuple<T, std::tuple<U, Types...>>
{
    static constexpr int Value = 1 + TIndexInTuple<T, std::tuple<Types...>>::Value;
};

template <class T, class TTuple>
constexpr bool DoesTupleContainType = (TIndexInTuple<T, TTuple>::Value < std::tuple_size<TTuple>{});

template <class TOut, class TIn = std::tuple<>>
struct TUniqueTypes;

template <class... TOut, class TInCar, class... TInCdr>
struct TUniqueTypes<std::tuple<TOut...>, std::tuple<TInCar, TInCdr...>>
{
    using TType = std::conditional_t<
        DoesTupleContainType<TInCar, std::tuple<TOut...>>,
        typename TUniqueTypes<std::tuple<TOut...>, std::tuple<TInCdr...>>::TType,
        typename TUniqueTypes<std::tuple<TOut..., TInCar>, std::tuple<TInCdr...>>::TType
    >;
};

template <class TOut>
struct TUniqueTypes<TOut, std::tuple<>>
{
    using TType = TOut;
};

} // namespace NDetail

/// @endcond Doxygen_Suppress

////////////////////////////////////////////////////////////////////////////////

}
