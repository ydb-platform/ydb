#ifndef STAN_MATH_FWD_SCAL_FUN_SINH_HPP
#define STAN_MATH_FWD_SCAL_FUN_SINH_HPP

#include <stan/math/fwd/core.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> sinh(const fvar<T>& x) {
  using std::cosh;
  using std::sinh;
  return fvar<T>(sinh(x.val_), x.d_ * cosh(x.val_));
}
}  // namespace math
}  // namespace stan
#endif
