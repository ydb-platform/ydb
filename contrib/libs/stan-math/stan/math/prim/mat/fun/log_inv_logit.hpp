#ifndef STAN_MATH_PRIM_MAT_FUN_LOG_INV_LOGIT_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG_INV_LOGIT_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap log_inv_logit() so it can be vectorized.
 */
struct log_inv_logit_fun {
  /**
   * Return the natural logarithm of the inverse logit
   * of the specified argument.
   *
   * @tparam T argument scalar type
   * @param x argument
   * @return natural log of inverse logit of argument
   */
  template <typename T>
  static inline T fun(const T& x) {
    return log_inv_logit(x);
  }
};

/**
 * Return the elementwise application of
 * <code>log_inv_logit()</code> to specified argument container.
 * The return type promotes the underlying scalar argument type to
 * double if it is an integer, and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise log_inv_logit of members of container
 */
template <typename T>
inline typename apply_scalar_unary<log_inv_logit_fun, T>::return_t
log_inv_logit(const T& x) {
  return apply_scalar_unary<log_inv_logit_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
