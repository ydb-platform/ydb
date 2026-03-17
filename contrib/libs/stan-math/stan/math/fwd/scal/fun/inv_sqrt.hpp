#ifndef STAN_MATH_FWD_SCAL_FUN_INV_SQRT_HPP
#define STAN_MATH_FWD_SCAL_FUN_INV_SQRT_HPP

#include <stan/math/fwd/core.hpp>

#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> inv_sqrt(const fvar<T>& x) {
  using std::sqrt;
  T sqrt_x(sqrt(x.val_));
  return fvar<T>(1 / sqrt_x, -0.5 * x.d_ / (x.val_ * sqrt_x));
}
}  // namespace math
}  // namespace stan
#endif
