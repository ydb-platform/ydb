#ifndef STAN_MATH_FWD_SCAL_FUN_FLOOR_HPP
#define STAN_MATH_FWD_SCAL_FUN_FLOOR_HPP

#include <stan/math/fwd/core.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> floor(const fvar<T>& x) {
  using std::floor;
  return fvar<T>(floor(x.val_), 0);
}

}  // namespace math
}  // namespace stan
#endif
