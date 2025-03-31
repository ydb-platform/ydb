#ifndef RANGE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include range_helpers.h"
// For the sake of sane code completion.
#include "range_helpers.h"
#endif

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
struct TAppendTo
{ };

template <class TContainer>
    requires requires (TContainer container, typename TContainer::value_type value) {
        container.push_back(value);
    }
struct TAppendTo<TContainer>
{
    template <class TValue>
    static void Append(TContainer& container, TValue&& value)
    {
        container.push_back(std::forward<TValue>(value));
    }
};

template <class TContainer>
    requires requires (TContainer container, typename TContainer::value_type value) {
        container.insert(value);
    }
struct TAppendTo<TContainer>
{
    template <class TValue>
    static void Append(TContainer& container, TValue&& value)
    {
        container.insert(std::forward<TValue>(value));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TContainer, std::ranges::input_range TRange>
struct TRangeTo
{ };

template <class TContainer, std::ranges::input_range TRange>
    requires requires (TContainer container, typename TContainer::value_type value) {
        TAppendTo<TContainer>::Append(container, value);
    }
struct TRangeTo<TContainer, TRange>
{
    static auto ToContainer(TRange&& range)
    {
        TContainer container;
        if constexpr (requires { std::ranges::size(range); } &&
            requires { container.reserve(std::declval<size_t>()); })
        {
            container.reserve(std::ranges::size(range));
        }

        for (auto&& element : range) {
            TAppendTo<TContainer>::Append(container, std::forward<decltype(element)>(element));
        }

        return container;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range... TContainers>
auto ZipMutable(TContainers&&... containers) {
    return Zip(std::ranges::views::transform(containers, [] <class T> (T&& t) {
        return &t;
    })...);
}

template <class TContainer, std::ranges::input_range TRange>
auto RangeTo(TRange&& range)
{
    return NDetail::TRangeTo<TContainer, TRange>::ToContainer(std::forward<TRange>(range));
}

template <class TContainer, std::ranges::input_range TRange, class TTransformFunction>
auto TransformRangeTo(TRange&& range, TTransformFunction&& function)
{
    return RangeTo<TContainer>(std::ranges::views::transform(
        std::forward<TRange>(range),
        std::forward<TTransformFunction>(function)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
