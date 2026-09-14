#pragma once

#include <cstddef>
#include <type_traits>

namespace NYql {

namespace NDetail {

struct TAny {
    template <typename T>
    consteval operator T() const { // NOLINT(google-explicit-constructor)
        return {};
    }
};

template <typename T, typename... Args>
consteval size_t FieldsCountGo(size_t count, Args... args) {
    static_assert(std::is_aggregate_v<T>);
    if constexpr (requires { T{args...}; }) {
        return FieldsCountGo<T>(count + 1, args..., TAny());
    } else {
        return count;
    }
}

template <typename T>
consteval std::size_t FieldsCount() {
    static_assert(std::is_aggregate_v<T>);
    return FieldsCountGo<T>(0, TAny());
}

} // namespace NDetail

template <typename T>
concept IsPod = (std::is_trivially_destructible<T>::value &&
                 std::is_trivially_copy_assignable<T>::value &&
                 std::is_trivially_move_assignable<T>::value &&
                 std::is_trivially_copy_constructible<T>::value &&
                 std::is_trivially_move_constructible<T>::value);

using NDetail::FieldsCount;

} // namespace NYql
