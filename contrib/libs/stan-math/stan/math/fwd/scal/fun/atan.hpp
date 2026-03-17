#ifndef STAN_MATH_FWD_SCAL_FUN_ATAN_HPP
#define STAN_MATH_FWD_SCAL_FUN_ATAN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> atan(const fvar<T>& x) {
  using std::atan;
  return fvar<T>(atan(x.val_), x.d_ / (1 + square(x.val_)));
}

}  // namespace math
}  // namespace stan
#endif
