#pragma once
#include <util/digest/multi.h>
#include <util/str_stl.h>
#include <util/stream/output.h>
#include <math.h>
#include <numeric>
#include <utility>

struct hash_combiner {
    template <typename T>
    inline static void hash_combine(std::size_t& seed, const T& val) {
        seed ^= std::hash<T>()(val) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

    template <typename T, typename... Ts>
    static void hash_combine(std::size_t& seed, const T& val, const Ts&... args)
    {
        hash_combine(seed, val);
        hash_combine(seed, args...);
    }

    static void hash_combine (std::size_t&) {}

    template <typename... Ts>
    static std::size_t hash_val(const Ts&... args)
    {
        std::size_t seed = 0;
        hash_combine(seed, args...);
        return seed;
    }
};

namespace std {

// std::hash for tuples
template <typename... Types>
struct hash<std::tuple<Types...>> {
    template <typename TupleType, std::size_t... I>
    inline static std::size_t MultiHasher(const TupleType& tuple, std::index_sequence<I...> ) {
        return hash_combiner::hash_val(std::get<I>(tuple)...);
    }

    inline size_t operator()(const std::tuple<Types...>& tuple) const {
        return MultiHasher(tuple, std::make_index_sequence<sizeof...(Types)>());
    }
};

// std::hash for pairs (subset of tuple)
template <typename TFirst, typename TSecond>
struct hash<pair<TFirst, TSecond>> {
    inline size_t operator()(pair<TFirst, TSecond> pair) const {
        return hash_combiner::hash_val(pair.first, pair.second);
    }
};

}

// output for tuples
template <typename TupleType, std::size_t N>
struct OutTuple : OutTuple<TupleType, N - 1> {
    static void Out(IOutputStream& stream, const TupleType& t) {
        OutTuple<TupleType, N - 1>::Out(stream, t);
        stream << "," << std::get<N>(t);
    }
};

template <typename TupleType>
struct OutTuple<TupleType, 0> {
    static void Out(IOutputStream& stream, const TupleType& t) {
        stream << std::get<0>(t);
    }
};

template <typename... Types>
IOutputStream& operator <<(IOutputStream& stream, const std::tuple<Types...>& t) {
    stream << "(";
    OutTuple<std::tuple<Types...>, sizeof...(Types) - 1>::Out(stream, t);
    stream << ")";
    return stream;
}

namespace NKikimr {

// tuple arithmethic operators (+ - * /)

template <std::size_t... I, typename... A, typename... B>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) + std::get<I>(std::tuple<B...>())...))
add(std::index_sequence<I...>, const std::tuple<A...>& a, const std::tuple<B...>& b) {
    return std::make_tuple(std::get<I>(a) + std::get<I>(b)...);
}

template <typename... A, typename... B>
decltype(add(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), std::tuple<B...>()))
operator +(const std::tuple<A...>& a, const std::tuple<B...>& b) {
    static_assert(sizeof...(A) == sizeof...(B), "Tuples should be the same size");
    return add(std::make_index_sequence<sizeof...(A)>(), a, b);
}

template <std::size_t... I, typename... A, typename... B>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) - std::get<I>(std::tuple<B...>())...))
sub(std::index_sequence<I...>, const std::tuple<A...>& a, const std::tuple<B...>& b) {
    return std::make_tuple(std::get<I>(a) - std::get<I>(b)...);
}

template <typename... A, typename... B>
decltype(sub(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), std::tuple<B...>()))
operator -(const std::tuple<A...>& a, const std::tuple<B...>& b) {
    static_assert(sizeof...(A) == sizeof...(B), "Tuples should be the same size");
    return sub(std::make_index_sequence<sizeof...(A)>(), a, b);
}

template <std::size_t... I, typename... A, typename... B>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) * std::get<I>(std::tuple<B...>())...))
mul(std::index_sequence<I...>, const std::tuple<A...>& a, const std::tuple<B...>& b) {
    return std::make_tuple(std::get<I>(a) * std::get<I>(b)...);
}

template <typename... A, typename... B>
decltype(mul(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), std::tuple<B...>()))
operator *(const std::tuple<A...>& a, const std::tuple<B...>& b) {
    static_assert(sizeof...(A) == sizeof...(B), "Tuples should be the same size");
    return mul(std::make_index_sequence<sizeof...(A)>(), a, b);
}

template <std::size_t... I, typename... A, typename... B>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) / std::get<I>(std::tuple<B...>())...))
div(std::index_sequence<I...>, const std::tuple<A...>& a, const std::tuple<B...>& b) {
    return std::make_tuple(std::get<I>(a) / std::get<I>(b)...);
}

template <typename... A, typename... B>
decltype(div(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), std::tuple<B...>()))
operator /(const std::tuple<A...>& a, const std::tuple<B...>& b) {
    static_assert(sizeof...(A) == sizeof...(B), "Tuples should be the same size");
    return div(std::make_index_sequence<sizeof...(A)>(), a, b);
}

// safe_div: same as operator/, but casts everything to double & returns 0 when denominator is 0

template <std::size_t... I, typename... A, typename... B>
auto safe_div(std::index_sequence<I...>, const std::tuple<A...>& a, const std::tuple<B...>& b) {
    return std::make_tuple(std::get<I>(b) ? static_cast<double>(std::get<I>(a)) / static_cast<double>(std::get<I>(b)) : double(0)...);
}

template <typename... A, typename... B>
auto safe_div(const std::tuple<A...> a, const std::tuple<B...> b) {
    static_assert(sizeof...(A) == sizeof...(B), "Tuples should be the same size");
    return safe_div(std::make_index_sequence<sizeof...(A)>(), a, b);
}

/////

template <std::size_t... I, typename... A, typename V>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) / V()...))
mul(std::index_sequence<I...>, const std::tuple<A...>& a, V b) {
    return std::make_tuple(std::get<I>(a) * b...);
}

template <typename... A, typename V>
decltype(mul(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), V()))
operator *(const std::tuple<A...>& a, V b) {
    return mul(std::make_index_sequence<sizeof...(A)>(), a, b);
}

template <std::size_t... I, typename... A, typename V>
decltype(std::make_tuple(std::get<I>(std::tuple<A...>()) / V()...))
div(std::index_sequence<I...>, const std::tuple<A...>& a, V b) {
    return std::make_tuple(std::get<I>(a) / b...);
}

template <typename... A, typename V>
decltype(div(std::make_index_sequence<sizeof...(A)>(), std::tuple<A...>(), V()))
operator /(const std::tuple<A...>& a, V b) {
    return div(std::make_index_sequence<sizeof...(A)>(), a, b);
}

/////

template <typename... A, typename... B>
std::tuple<A...> operator +=(std::tuple<A...>& a, const std::tuple<B...>& b) {
    return a = (a + b);
}

template <typename... A, typename... B>
std::tuple<A...> operator -=(std::tuple<A...>& a, const std::tuple<B...>& b) {
    return a = (a - b);
}

template <typename... A, typename... B>
std::tuple<A...> operator /=(std::tuple<A...>& a, const std::tuple<B...>& b) {
    return a = (a / b);
}

template <typename... A, typename... B>
std::tuple<A...> operator *=(std::tuple<A...>& a, const std::tuple<B...>& b) {
    return a = (a * b);
}

/////

template <typename... T, typename V>
decltype(std::tuple<T...>() / V()) operator /=(std::tuple<T...>& a, V b) {
    return a = (a / b);
}

// max(tuple<>) - returns maximum of all tuple elements

template <std::size_t... I, typename...T>
decltype(std::max({std::get<I>(std::tuple<T...>())...})) max(std::index_sequence<I...>, const std::tuple<T...>& a) {
    return std::max({std::get<I>(a)...});
}

template <typename... T>
decltype(max(std::make_index_sequence<sizeof...(T)>(), std::tuple<T...>())) max(const std::tuple<T...>& a) {
    return max(std::make_index_sequence<sizeof...(T)>(), a);
}

// min(tuple<>) - returns minimum of all tuple elements

template <std::size_t... I, typename...T>
decltype(std::min({std::get<I>(std::tuple<T...>())...})) min(std::index_sequence<I...>, const std::tuple<T...>& a) {
    return std::min({std::get<I>(a)...});
}

template <typename... T>
decltype(min(std::make_index_sequence<sizeof...(T)>(), std::tuple<T...>())) min(const std::tuple<T...>& a) {
    return min(std::make_index_sequence<sizeof...(T)>(), a);
}

// sqrt(tuple<>) - returns sqrt of every tuple element

template <std::size_t... I, typename...T>
decltype(std::make_tuple(::sqrt(std::get<I>(std::tuple<T...>()))...)) sqrt(std::index_sequence<I...>, const std::tuple<T...>& a) {
    return std::make_tuple(::sqrt(std::get<I>(a))...);
}

template <typename... T>
decltype(sqrt(std::make_index_sequence<sizeof...(T)>(), std::tuple<T...>())) sqrt(const std::tuple<T...>& a) {
    return sqrt(std::make_index_sequence<sizeof...(T)>(), a);
}

// convert(tuple<>, f) - converts every tuple element using f
// convert(tuple<> a, tuple<> b, f) - returns a tuple of f(a_i, b_i)

template <std::size_t... I, typename...T, typename F>
decltype(std::make_tuple((*(F*)(nullptr))(std::get<I>(std::tuple<T...>()))...)) convert(std::index_sequence<I...>, const std::tuple<T...>& a, F f) {
    return std::make_tuple(f(std::get<I>(a))...);
}

template <typename... T, typename F>
decltype(convert(std::make_index_sequence<sizeof...(T)>(), std::tuple<T...>(), *(F*)(nullptr))) convert(const std::tuple<T...>& a, F f) {
    return convert(std::make_index_sequence<sizeof...(T)>(), a, f);
}

template<std::size_t... I, typename... T, typename... U, typename F>
auto convert(std::index_sequence<I...>, const std::tuple<T...>& a, const std::tuple<U...>& b, F f) {
    return std::make_tuple(f(std::get<I>(a), std::get<I>(b))...);
}

template <typename... T, typename... U, typename F>
auto convert(const std::tuple<T...>& a, const std::tuple<T...>& b, F f) {
    return convert(std::make_index_sequence<sizeof...(T)>(), a, b, f);
}

template <typename... T>
struct tuple_cast {
    template <std::size_t... I, typename... F>
    static std::tuple<T...> cast(std::index_sequence<I...>, const std::tuple<F...>& a) {
        return std::tuple<T...>((T)std::get<I>(a)...);
    }

    template <typename... F>
    static std::tuple<T...> cast(const std::tuple<F...>& a) {
        static_assert(sizeof...(F) == sizeof...(T), "Tuples should be equal size");
        return cast(std::make_index_sequence<sizeof...(T)>(), a);
    }
};

// sum(tuple<>) - returns sum of all tuple elements

template <typename T>
T sum(const std::initializer_list<T>& v) {
    return std::accumulate(v.begin(), v.end(), T());
}

template <std::size_t... I, typename...T>
decltype(sum({std::get<I>(std::tuple<T...>())...})) sum(std::index_sequence<I...>, const std::tuple<T...>& a) {
    return sum({std::get<I>(a)...});
}

template <typename... T>
decltype(sum(std::make_index_sequence<sizeof...(T)>(), std::tuple<T...>())) sum(const std::tuple<T...>& a) {
    return sum(std::make_index_sequence<sizeof...(T)>(), a);
}

// make_n_tuple<N,T>::type - returns type of std::tuple<T, T, T... N times>

template <typename T1, typename T2>
struct concat_tuples;
template <typename... T1, typename... T2>
struct concat_tuples<std::tuple<T1...>, std::tuple<T2...>> {
    using type = std::tuple<T1..., T2...>;
};
template <std::size_t N, typename T>
struct make_n_tuple {
    using type = typename concat_tuples<std::tuple<T>, typename make_n_tuple<N - 1, T>::type>::type;
};
template <typename T>
struct make_n_tuple<1, T> {
    using type = std::tuple<T>;
};

// make_array(tuple<>) - converts to std::array<>

template <typename T, std::size_t... I>
std::array<std::tuple_element_t<0, T>, std::tuple_size<T>::value> tuple_to_array(std::index_sequence<I...>, const T& a) {
    return {{std::get<I>(a)...}};
}

template <typename T>
std::array<std::tuple_element_t<0, T>, std::tuple_size<T>::value> tuple_to_array(const T& a) {
    return tuple_to_array(std::make_index_sequence<std::tuple_size<T>::value>(), a);
}

// make_tuple(array<>) - converts to std::tuple<>

template <typename T, std::size_t S, std::size_t... I>
typename make_n_tuple<S, T>::type array_to_tuple(std::index_sequence<I...>, const std::array<T, S>& a) {
    return std::make_tuple(a[I]...);
}

template <typename T, std::size_t S>
typename make_n_tuple<S, T>::type array_to_tuple(const std::array<T, S>& a) {
    return array_to_tuple(std::make_index_sequence<S>(), a);
}

// index_of

template <typename T, typename Tuple>
struct index_of;
template <typename T, typename... Types>
struct index_of<T, std::tuple<T, Types...>> {
    static constexpr std::size_t value = 0;
};
template <typename T, typename U, class... Types>
struct index_of<T, std::tuple<U, Types...>> {
    static constexpr std::size_t value = 1 + index_of<T, std::tuple<Types...>>::value;
};

// first_n_of

template <std::size_t, typename...>
struct first_n_of_tuple;
template <std::size_t N, typename Type, typename... Types>
struct first_n_of_tuple<N, Type, Types...> {
    using type = typename concat_tuples<std::tuple<Type>, typename first_n_of_tuple<N - 1, Types...>::type>::type;
};
template <typename... Types>
struct first_n_of_tuple<0, Types...> {
    using type = std::tuple<>;
};
template <typename Type, typename... Types>
struct first_n_of_tuple<1, Type, Types...> {
    using type = std::tuple<Type>;
};

template <std::size_t, typename...>
struct first_n_of;
template <typename... Types>
struct first_n_of<0, std::tuple<Types...>> {
    using type = std::tuple<>;
};
template <std::size_t N, typename... Types>
struct first_n_of<N, std::tuple<Types...>> {
    using type = typename first_n_of_tuple<N, Types...>::type;
};

// piecewise_max(tuple<>, tuple<>)

template <std::size_t... I, typename... Ts>
inline std::tuple<Ts...> piecewise_max(std::index_sequence<I...>, const std::tuple<Ts...>& a, const std::tuple<Ts...>& b) { return std::tuple<Ts...>({std::max<Ts>(std::get<I>(a), std::get<I>(b))...}); }
template <typename... Ts>
inline std::tuple<Ts...> piecewise_max(const std::tuple<Ts...>& a, const std::tuple<Ts...>& b) { return piecewise_max(std::make_index_sequence<sizeof...(Ts)>(), a, b); }

// piecewise_min(tuple<>, tuple<>)

template <std::size_t... I, typename... Ts>
inline std::tuple<Ts...> piecewise_min(std::index_sequence<I...>, const std::tuple<Ts...>& a, const std::tuple<Ts...>& b) { return std::tuple<Ts...>({std::min<Ts>(std::get<I>(a), std::get<I>(b))...}); }
template <typename... Ts>
inline std::tuple<Ts...> piecewise_min(const std::tuple<Ts...>& a, const std::tuple<Ts...>& b) { return piecewise_min(std::make_index_sequence<sizeof...(Ts)>(), a, b); }

// piecewise_compare(tuple<>, tuple<>) -> tuple<ordering>

template <std::size_t... I, typename... Ts>
inline auto piecewise_compare(std::index_sequence<I...>, const std::tuple<Ts...>&a, const std::tuple<Ts...>& b) {
    return std::make_tuple((std::get<I>(a) <=> std::get<I>(b))...);
}
template <typename... Ts>
inline auto piecewise_compare(const std::tuple<Ts...>& a, const std::tuple<Ts...>& b) {
    return piecewise_compare(std::make_index_sequence<sizeof...(Ts)>(), a, b);
}

}
