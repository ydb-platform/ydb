#pragma once

#include "public.h"
#include "error_attribute.h"

#include <ranges>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Can be specialized to make your dictionary satisfy CMergeableDictionary.
template <class TDictionary>
struct TMergeDictionariesTraits
{
    static auto MakeIterableView(const TDictionary& dict)
        requires false;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
struct TMergeableDictionaryImpl
{
    // TL;DR: MakeIterableView returns something like std::span<std::pair<TKey, TValue>>.
    using TView = std::invoke_result_t<decltype(&TMergeDictionariesTraits<T>::MakeIterableView), const T&>;
    using TIterator = std::ranges::iterator_t<TView>;
    using TValue = typename std::iterator_traits<TIterator>::value_type;

    static constexpr bool ValidSize = requires {
        { std::tuple_size<TValue>::value } -> std::same_as<const size_t&>;
    } && (std::tuple_size<TValue>::value == 2);

    static constexpr bool CorrectTupleElements = requires {
        typename std::tuple_element<0, TValue>::type;
        std::same_as<typename std::tuple_element<0, TValue>::type, TErrorAttribute::TKey>;

        typename std::tuple_element<1, TValue>::type;
        std::same_as<typename std::tuple_element<1, TValue>::type, TErrorAttribute::TValue>;
    };
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CMergeableDictionary =
    requires (const T& dict) {
        TMergeDictionariesTraits<T>::MakeIterableView(dict);
    } &&
    NDetail::TMergeableDictionaryImpl<T>::ValidSize &&
    NDetail::TMergeableDictionaryImpl<T>::CorrectTupleElements;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
