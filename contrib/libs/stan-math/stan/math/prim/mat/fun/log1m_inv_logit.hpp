#ifndef STAN_MATH_PRIM_MAT_FUN_LOG1M_INV_LOGIT_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG1M_INV_LOGIT_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/log1m_inv_logit.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap log1m_inv_logit() so it can be vectorized.
 */
struct log1m_inv_logit_fun {
  /**
   * Return the natural logarithm of one minus the inverse logit
   * of the specified argument.
   *
   * @tparam T argument scalar type
   * @param x argument
   * @return natural log of one minus inverse logit of argument
   */
  template <typename T>
  static inline T fun(const T& x) {
    return log1m_inv_logit(x);
  }
};

/**
 * Return the elementwise application of
 * <code>log1m_inv_logit()</code> to specified argument container.
 * The return type promotes the underlying scalar argument type to
 * double if it is an integer, and otherwise is the argument type.
 *
 * @tparam T Container type.
 * @param x Container.
 * @return Elementwise log1m_inv_logit of members of container.
 */
template <typename T>
inline typename apply_scalar_unary<log1m_inv_logit_fun, T>::return_t
log1m_inv_logit(const T& x) {
  return apply_scalar_unary<log1m_inv_logit_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
