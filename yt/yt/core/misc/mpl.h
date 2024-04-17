#pragma once

#include <util/generic/typetraits.h>

#include <tuple>
#include <type_traits>

// See the following references for an inspiration:
//   * http://llvm.org/viewvc/llvm-project/libcxx/trunk/include/type_traits?revision=HEAD&view=markup
//   * http://www.boost.org/doc/libs/1_48_0/libs/type_traits/doc/html/index.html
//   * http://www.boost.org/doc/libs/1_48_0/libs/mpl/doc/index.html

namespace NYT::NMpl {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, bool isPrimitive>
struct TCallTraitsHelper
{  };

template <class T>
struct TCallTraitsHelper<T, true>
{
    using TType = T;
};

template <class T>
struct TCallTraitsHelper<T, false>
{
    using TType = const T&;
};

template <template <class...> class TTemplate, class... TArgs>
void DerivedFromSpecializationImpl(const TTemplate<TArgs...>&);

} // namespace NDetail

//! A trait for choosing appropriate argument and return types for functions.
/*!
 *  All types except for primitive ones should be passed to functions
 *  and returned from const getters by const ref.
 */
template <class T>
struct TCallTraits
    : public NDetail::TCallTraitsHelper<T, !std::is_class<T>::value>
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIsPod
    : std::integral_constant<bool, ::TTypeTraits<T>::IsPod>
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, template <class...> class TTemplatedBase>
concept DerivedFromSpecializationOf = requires(const TDerived& instance)
{
    NDetail::DerivedFromSpecializationImpl<TTemplatedBase>(instance);
};

////////////////////////////////////////////////////////////////////////////////

// Inspired by https://stackoverflow.com/questions/51032671/idiomatic-way-to-write-concept-that-says-that-type-is-a-stdvector
template<class, template<class...> class>
inline constexpr bool IsSpecialization = false;
template<template<class...> class T, class... Args>
inline constexpr bool IsSpecialization<T<Args...>, T> = true;

////////////////////////////////////////////////////////////////////////////////

template <class TNeedle, class... THayStack>
concept COneOf = (std::same_as<TNeedle, THayStack> || ...);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename... Ts>
inline constexpr bool DistinctImpl = true;

template <typename T, typename... Ts>
inline constexpr bool DistinctImpl<T, Ts...> = DistinctImpl<Ts...> && !COneOf<T, Ts...>;

} // namespace NDetail

template <typename... Ts>
concept CDistinct = NDetail::DistinctImpl<Ts...>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMpl
