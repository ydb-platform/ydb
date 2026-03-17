#ifndef STAN_MATH_PRIM_MAT_FUN_PROMOTE_COMMON_HPP
#define STAN_MATH_PRIM_MAT_FUN_PROMOTE_COMMON_HPP

#include <stan/math/prim/mat/fun/common_type.hpp>
#include <stan/math/prim/mat/fun/promote_elements.hpp>

namespace stan {
namespace math {

/**
 * Return the result of promoting either a scalar or the scalar elements
 * of a container to either of two specified types, as determined
 * by stan::math::common_type.
 *
 * @tparam T1 first type
 * @tparam T2 second type
 * @tparam F type of container elements, must be either T1 or T2
 * @param u elements to promote
 * @return the result of promoting elements
 */
template <typename T1, typename T2, typename F>
inline typename common_type<T1, T2>::type promote_common(const F& u) {
  return promote_elements<typename common_type<T1, T2>::type, F>::promote(u);
}

}  // namespace math
}  // namespace stan

#endif
