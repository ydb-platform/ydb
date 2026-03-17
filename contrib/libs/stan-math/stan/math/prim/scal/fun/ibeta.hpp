#ifndef STAN_MATH_PRIM_SCAL_FUN_IBETA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IBETA_HPP

#include <boost/math/special_functions/beta.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>

namespace stan {
namespace math {

/**
 * The normalized incomplete beta function of a, b, and x.
 *
 * Used to compute the cumulative density function for the beta
 * distribution.
 *
 * @param a Shape parameter a <= 0; a and b can't both be 0
 * @param b Shape parameter b <= 0
 * @param x Random variate. 0 <= x <= 1
 * @throws if constraints are violated or if any argument is NaN
 *
 * @return The normalized incomplete beta function.
 */
inline double ibeta(double a, double b, double x) {
  check_not_nan("ibeta", "a", a);
  check_not_nan("ibeta", "b", b);
  check_not_nan("ibeta", "x", x);
  return boost::math::ibeta(a, b, x);
}

}  // namespace math
}  // namespace stan
#endif
