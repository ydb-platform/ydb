#ifndef STAN_MATH_PRIM_MAT_FUN_ACOS_HPP
#define STAN_MATH_PRIM_MAT_FUN_ACOS_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap acos() so it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Arc cosine of variable in radians.
 */
struct acos_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::acos;
    return acos(x);
  }
};

/**
 * Vectorized version of acos().
 * @param x Container of variables.
 * @tparam T Container type.
 * @return Arc cosine of each variable in the container, in radians.
 */
template <typename T>
inline typename apply_scalar_unary<acos_fun, T>::return_t acos(const T& x) {
  return apply_scalar_unary<acos_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
