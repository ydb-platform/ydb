#ifndef STAN_MATH_FWD_SCAL_FUN_PHI_APPROX_HPP
#define STAN_MATH_FWD_SCAL_FUN_PHI_APPROX_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return an approximation of the unit normal cumulative
 * distribution function (CDF).
 *
 * @tparam T scalar type of forward-mode autodiff variable
 * argument.
 * @param x argument
 * @return approximate probability random sample is less than or
 * equal to argument
 */
template <typename T>
inline fvar<T> Phi_approx(const fvar<T>& x) {
  using std::pow;
  return inv_logit(0.07056 * pow(x, 3.0) + 1.5976 * x);
}

}  // namespace math
}  // namespace stan
#endif
