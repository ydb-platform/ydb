#ifndef RANGE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include range_helpers.h"
// For the sake of sane code completion.
#include "range_helpers.h"
#endif

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer, std::ranges::input_range TRange>
struct TRangeTo
{ };

template <class TContainer, std::ranges::input_range TRange>
    requires requires (TContainer container, size_t size) {
        container.reserve(size);
        container.push_back(std::declval<typename TContainer::value_type>());
    }
struct TRangeTo<TContainer, TRange>
{
    static auto ToContainer(TRange&& range)
    {
        TContainer container;
        if constexpr (requires { std::ranges::size(range); }) {
            container.reserve(std::ranges::size(range));
        }

        for (auto&& element : range) {
            container.push_back(std::forward<decltype(element)>(element));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
