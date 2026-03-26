#pragma once

#include <type_traits>

namespace NYql {

template <typename T>
concept IsPod = (std::is_trivially_destructible<T>::value &&
                 std::is_trivially_copy_assignable<T>::value &&
                 std::is_trivially_move_assignable<T>::value &&
                 std::is_trivially_copy_constructible<T>::value &&
                 std::is_trivially_move_constructible<T>::value);

} // namespace NYql
