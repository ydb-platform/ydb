#ifndef STAN_MATH_FWD_SCAL_FUN_COS_HPP
#define STAN_MATH_FWD_SCAL_FUN_COS_HPP

#include <stan/math/fwd/core.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> cos(const fvar<T>& x) {
  using std::cos;
  using std::sin;
  return fvar<T>(cos(x.val_), x.d_ * -sin(x.val_));
}

}  // namespace math
}  // namespace stan
#endif
