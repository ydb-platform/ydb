#ifndef STAN_MATH_PRIM_MAT_FUN_ASIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_ASIN_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap asin() so it can be vectorized.
 * @param x Argument variable.
 * @tparam T Argument type.
 * @return Arcsine of x in radians.
 */
struct asin_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::asin;
    return asin(x);
  }
};

/**
 * Vectorized version of asin().
 * @param x Container of variables.
 * @tparam T Container type.
 * @return Arcsine of each variable in the container, in radians.
 */
template <typename T>
inline typename apply_scalar_unary<asin_fun, T>::return_t asin(const T& x) {
  return apply_scalar_unary<asin_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
