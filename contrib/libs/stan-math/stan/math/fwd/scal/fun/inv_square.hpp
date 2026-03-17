#ifndef STAN_MATH_FWD_SCAL_FUN_INV_SQUARE_HPP
#define STAN_MATH_FWD_SCAL_FUN_INV_SQUARE_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/square.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> inv_square(const fvar<T>& x) {
  T square_x(square(x.val_));
  return fvar<T>(1 / square_x, -2 * x.d_ / (square_x * x.val_));
}
}  // namespace math
}  // namespace stan
#endif
