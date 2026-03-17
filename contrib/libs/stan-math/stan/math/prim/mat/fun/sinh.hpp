#ifndef STAN_MATH_PRIM_MAT_FUN_SINH_HPP
#define STAN_MATH_PRIM_MAT_FUN_SINH_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap sinh() so that it can be vectorized.
 * @param x Angle in radians.
 * @tparam T Variable type.
 * @return Hyperbolic sine of x.
 */
struct sinh_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::sinh;
    return sinh(x);
  }
};

/**
 * Vectorized version of sinh().
 * @param x Container of variables.
 * @tparam T Container type.
 * @return Hyperbolic sine of each variable in x.
 */
template <typename T>
inline typename apply_scalar_unary<sinh_fun, T>::return_t sinh(const T& x) {
  return apply_scalar_unary<sinh_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
