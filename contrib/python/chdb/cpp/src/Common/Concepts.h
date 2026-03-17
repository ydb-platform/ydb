#pragma once

#include <concepts>

namespace DB_CHDB
{

template<typename T, typename ... U>
concept is_any_of = (std::same_as<T, U> || ...);


template <typename... T>
concept OptionalArgument = requires(T &&...)
{
    requires(sizeof...(T) == 0 || sizeof...(T) == 1);
};

}
