#ifndef STAN_MATH_PRIM_MAT_FUN_PHI_APPROX_HPP
#define STAN_MATH_PRIM_MAT_FUN_PHI_APPROX_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/Phi_approx.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap Phi_approx() so it can be vectorized.
 */
struct Phi_approx_fun {
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
    return Phi_approx(x);
  }
};

/**
 * Return the elementwise application of <code>Phi_approx()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise Phi_approx of container elements
 */
template <typename T>
inline typename apply_scalar_unary<Phi_approx_fun, T>::return_t Phi_approx(
    const T& x) {
  return apply_scalar_unary<Phi_approx_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
