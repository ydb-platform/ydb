#ifndef STAN_MATH_FWD_SCAL_FUN_ASIN_HPP
#define STAN_MATH_FWD_SCAL_FUN_ASIN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> asin(const fvar<T>& x) {
  using std::asin;
  using std::sqrt;
  return fvar<T>(asin(x.val_), x.d_ / sqrt(1 - square(x.val_)));
}

}  // namespace math
}  // namespace stan
#endif
