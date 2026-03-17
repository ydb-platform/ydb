#ifndef STAN_MATH_FWD_SCAL_FUN_PHI_HPP
#define STAN_MATH_FWD_SCAL_FUN_PHI_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/Phi.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> Phi(const fvar<T>& x) {
  using std::exp;
  using std::sqrt;
  T xv = x.val_;
  return fvar<T>(Phi(xv), x.d_ * exp(xv * xv / -2.0) / sqrt(2.0 * pi()));
}

}  // namespace math
}  // namespace stan
#endif
