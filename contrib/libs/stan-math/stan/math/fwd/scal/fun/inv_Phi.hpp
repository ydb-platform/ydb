#ifndef STAN_MATH_FWD_SCAL_FUN_INV_PHI_HPP
#define STAN_MATH_FWD_SCAL_FUN_INV_PHI_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/inv_Phi.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/square.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> inv_Phi(const fvar<T>& p) {
  using std::exp;
  T xv = inv_Phi(p.val_);
  return fvar<T>(xv, p.d_ / exp(-0.5 * square(xv)) * SQRT_2_TIMES_SQRT_PI);
}
}  // namespace math
}  // namespace stan
#endif
