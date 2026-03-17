#ifndef STAN_MATH_PRIM_ARR_META_CONTAINS_STD_VECTOR_HPP
#define STAN_MATH_PRIM_ARR_META_CONTAINS_STD_VECTOR_HPP

#include <stan/math/prim/scal/meta/contains_std_vector.hpp>
#include <type_traits>
#include <vector>

namespace stan {
/**
 * Extends std::true_type when instantiated with at least 1 template
 * parameter of type std::vector<T>.
 */
template <typename T, typename... Ts>
struct contains_std_vector<std::vector<T>, Ts...> : std::true_type {};

template <typename T, typename... Ts>
struct contains_std_vector<T, Ts...> : contains_std_vector<Ts...> {};

}  // namespace stan
#endif
