#ifndef STAN_MATH_FWD_SCAL_FUN_TANH_HPP
#define STAN_MATH_FWD_SCAL_FUN_TANH_HPP

#include <stan/math/fwd/core.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> tanh(const fvar<T>& x) {
  using std::tanh;
  T u = tanh(x.val_);
  return fvar<T>(u, x.d_ * (1 - u * u));
}
}  // namespace math
}  // namespace stan
#endif
