#ifndef STAN_MATH_FWD_SCAL_FUN_EXP2_HPP
#define STAN_MATH_FWD_SCAL_FUN_EXP2_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/exp2.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> exp2(const fvar<T>& x) {
  using std::log;
  return fvar<T>(exp2(x.val_), x.d_ * exp2(x.val_) * LOG_2);
}

}  // namespace math
}  // namespace stan
#endif
