#ifndef STAN_MATH_FWD_SCAL_FUN_CEIL_HPP
#define STAN_MATH_FWD_SCAL_FUN_CEIL_HPP

#include <stan/math/fwd/core.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> ceil(const fvar<T>& x) {
  using std::ceil;
  return fvar<T>(ceil(x.val_), 0);
}

}  // namespace math
}  // namespace stan
#endif
