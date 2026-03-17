#ifndef STAN_MATH_PRIM_MAT_FUN_TRIGAMMA_HPP
#define STAN_MATH_PRIM_MAT_FUN_TRIGAMMA_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/trigamma.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap trigamma() so it can be vectorized.
 */
struct trigamma_fun {
  /**
   * Return the approximate value of the Phi() function applied to
   * the argument.
   *
   * @tparam T argument type
   * @param x argument
   * @return aprpoximate value of Phi applied to argument.
   */
  template <typename T>
  static inline T fun(const T& x) {
    return trigamma(x);
  }
};

/**
 * Return the elementwise application of <code>trigamma()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise trigamma of container elements
 */
template <typename T>
inline typename apply_scalar_unary<trigamma_fun, T>::return_t trigamma(
    const T& x) {
  return apply_scalar_unary<trigamma_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
