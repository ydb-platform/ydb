#pragma once

#include <util/generic/maybe.h>

namespace NYql {

template <typename T>
constexpr bool IsMaybeV = false;

template <typename T>
constexpr bool IsMaybeV<TMaybe<T>> = true;

} // namespace NYql
