#ifndef STAN_MATH_PRIM_SCAL_META_CONTAINS_STD_VECTOR_HPP
#define STAN_MATH_PRIM_SCAL_META_CONTAINS_STD_VECTOR_HPP

#include <stan/math/prim/scal/meta/contains_std_vector.hpp>
#include <type_traits>

namespace stan {
/**
 * Extends std::false_type as a std::vector type
 * cannot be a scalar primitive type.
 * @tparam Ts Types to test
 */
template <typename... Ts>
struct contains_std_vector : std::false_type {};
}  // namespace stan

#endif
