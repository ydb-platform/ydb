#ifndef STAN_MATH_FWD_SCAL_FUN_INV_LOGIT_HPP
#define STAN_MATH_FWD_SCAL_FUN_INV_LOGIT_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/inv_logit.hpp>

namespace stan {
namespace math {

/**
 * Returns the inverse logit function applied to the argument.
 *
 * @tparam T scalar type of forward-mode autodiff variable
 * argument.
 * @param x argument
 * @return inverse logit of argument
 */
template <typename T>
inline fvar<T> inv_logit(const fvar<T>& x) {
  using std::exp;
  using std::pow;
  return fvar<T>(inv_logit(x.val_),
                 x.d_ * inv_logit(x.val_) * (1 - inv_logit(x.val_)));
}
}  // namespace math
}  // namespace stan
#endif
