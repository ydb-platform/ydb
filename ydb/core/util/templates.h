#pragma once

namespace NKikimr {

// first_of

template <typename T, typename... Types>
struct first_of {
    using type = T;
};

// max_of

template <std::size_t... I>
struct max_of;
template <std::size_t I>
struct max_of<I> {
    static constexpr std::size_t value = I;
};
template <std::size_t A, std::size_t B>
struct max_of<A, B> {
    static constexpr std::size_t value = A > B ? A : B;
};
template <std::size_t A, std::size_t B, std::size_t... I>
struct max_of<A, B, I...> {
    static constexpr std::size_t value = max_of<max_of<A, B>::value, I...>::value;
};

namespace SFINAE {

template <typename>
struct type_check {
    using type = int;
};

struct general {};
struct special : general {};

}

}
