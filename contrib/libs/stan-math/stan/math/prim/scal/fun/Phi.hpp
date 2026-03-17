#ifndef STAN_MATH_PRIM_SCAL_FUN_PHI_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PHI_HPP

#include <stan/math/prim/scal/fun/erf.hpp>
#include <stan/math/prim/scal/fun/erfc.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>

namespace stan {
namespace math {

/**
 * The unit normal cumulative distribution function.
 *
 * The return value for a specified input is the probability that
 * a random unit normal variate is less than or equal to the
 * specified value, defined by
 *
 * \f$\Phi(x) = \int_{-\infty}^x \mbox{\sf Norm}(x|0, 1) \ dx\f$
 *
 * This function can be used to implement the inverse link function
 * for probit regression.
 *
 * Phi will underflow to 0 below -37.5 and overflow to 1 above 8
 *
 * @param x Argument.
 * @return Probability random sample is less than or equal to argument.
 */
inline double Phi(double x) {
  check_not_nan("Phi", "x", x);
  if (x < -37.5)
    return 0;
  else if (x < -5.0)
    return 0.5 * erfc(-INV_SQRT_2 * x);
  else if (x > 8.25)
    return 1;
  else
    return 0.5 * (1.0 + erf(INV_SQRT_2 * x));
}

}  // namespace math
}  // namespace stan
#endif
