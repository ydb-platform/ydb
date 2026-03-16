#ifndef STAN_MATH_FWD_SCAL_FUN_LOG1M_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG1M_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> log1m(const fvar<T>& x) {
  return fvar<T>(log1m(x.val_), -x.d_ / (1 - x.val_));
}
}  // namespace math
}  // namespace stan
#endif
