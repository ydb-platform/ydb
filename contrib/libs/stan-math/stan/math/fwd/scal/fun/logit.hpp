#ifndef STAN_MATH_FWD_SCAL_FUN_LOGIT_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOGIT_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/logit.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> logit(const fvar<T>& x) {
  if (x.val_ > 1 || x.val_ < 0)
    return fvar<T>(NOT_A_NUMBER, NOT_A_NUMBER);
  else
    return fvar<T>(logit(x.val_), x.d_ / (x.val_ - square(x.val_)));
}
}  // namespace math
}  // namespace stan
#endif
