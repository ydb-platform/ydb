#ifndef STAN_MATH_FWD_SCAL_FUN_LOG1M_INV_LOGIT_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG1M_INV_LOGIT_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/log1m_inv_logit.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of one minus the inverse logit of
 * the specified argument.
 *
 * @tparam T scalar type of forward-mode autodiff variable
 * argument.
 * @param x argument
 * @return log of one minus the inverse logit of the argument
 */
template <typename T>
inline fvar<T> log1m_inv_logit(const fvar<T>& x) {
  using std::exp;
  return fvar<T>(log1m_inv_logit(x.val_), -x.d_ / (1 + exp(-x.val_)));
}

}  // namespace math
}  // namespace stan
#endif
