#ifndef STAN_MATH_PRIM_MAT_FUN_TANH_HPP
#define STAN_MATH_PRIM_MAT_FUN_TANH_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap tanh() so that it can be vectorized.
 * @param x Angle in radians.
 * @tparam T Variable type.
 * @return Hyperbolic tangent of x.
 */
struct tanh_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::tanh;
    return tanh(x);
  }
};

/**
 * Vectorized version of tanh().
 * @param x Container of angles in radians.
 * @tparam T Container type.
 * @return Hyperbolic tangent of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<tanh_fun, T>::return_t tanh(const T& x) {
  return apply_scalar_unary<tanh_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
