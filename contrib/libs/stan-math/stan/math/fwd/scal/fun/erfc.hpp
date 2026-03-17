#ifndef STAN_MATH_FWD_SCAL_FUN_ERFC_HPP
#define STAN_MATH_FWD_SCAL_FUN_ERFC_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/erfc.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> erfc(const fvar<T>& x) {
  using std::exp;
  return fvar<T>(erfc(x.val_), -x.d_ * exp(-square(x.val_)) * TWO_OVER_SQRT_PI);
}

}  // namespace math
}  // namespace stan
#endif
