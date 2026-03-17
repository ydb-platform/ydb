#ifndef STAN_MATH_PRIM_MAT_FUN_TAN_HPP
#define STAN_MATH_PRIM_MAT_FUN_TAN_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap tan() so that it can be vectorized.
 * @param x Angle in radians.
 * @tparam T Variable type.
 * @return Tangent of x.
 */
struct tan_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::tan;
    return tan(x);
  }
};

/**
 * Vectorized version of tan().
 * @param x Container of angles in radians.
 * @tparam T Container type.
 * @return Tangent of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<tan_fun, T>::return_t tan(const T& x) {
  return apply_scalar_unary<tan_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
