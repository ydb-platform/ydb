#ifndef STAN_MATH_PRIM_MAT_FUN_COSH_HPP
#define STAN_MATH_PRIM_MAT_FUN_COSH_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap cosh() so it can be vectorized.
 * @param x Angle in radians.
 * @tparam T Variable type.
 * @return Hyperbolic cosine of x.
 */
struct cosh_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::cosh;
    return cosh(x);
  }
};

/**
 * Vectorized version of cosh().
 * @param x Angle in radians.
 * @tparam T Variable type.
 * @return Hyberbolic cosine of x.
 */
template <typename T>
inline typename apply_scalar_unary<cosh_fun, T>::return_t cosh(const T& x) {
  return apply_scalar_unary<cosh_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
