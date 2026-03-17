#ifndef STAN_MATH_FWD_SCAL_FUN_COSH_HPP
#define STAN_MATH_FWD_SCAL_FUN_COSH_HPP

#include <stan/math/fwd/core.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> cosh(const fvar<T>& x) {
  using std::cosh;
  using std::sinh;
  return fvar<T>(cosh(x.val_), x.d_ * sinh(x.val_));
}

}  // namespace math
}  // namespace stan
#endif
