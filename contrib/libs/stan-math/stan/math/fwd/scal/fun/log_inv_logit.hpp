#ifndef STAN_MATH_FWD_SCAL_FUN_LOG_INV_LOGIT_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG_INV_LOGIT_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/log_inv_logit.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> log_inv_logit(const fvar<T>& x) {
  using std::exp;
  return fvar<T>(log_inv_logit(x.val_), x.d_ / (1 + exp(x.val_)));
}
}  // namespace math
}  // namespace stan
#endif
