#ifndef STAN_MATH_PRIM_SCAL_FUN_PHI_APPROX_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PHI_APPROX_HPP

#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return an approximation of the unit normal CDF.
 *
 * http://www.jiem.org/index.php/jiem/article/download/60/27
 *
 * This function can be used to implement the inverse link function
 * for probit regression.
 *
 * @param x Argument.
 * @return Probability random sample is less than or equal to argument.
 */
inline double Phi_approx(double x) {
  using std::pow;
  return inv_logit(0.07056 * pow(x, 3.0) + 1.5976 * x);
}

/**
 * Return an approximation of the unit normal CDF.
 *
 * @param x argument.
 * @return approximate probability random sample is less than or
 * equal to argument.
 */
inline double Phi_approx(int x) { return Phi_approx(static_cast<double>(x)); }

}  // namespace math
}  // namespace stan

#endif
