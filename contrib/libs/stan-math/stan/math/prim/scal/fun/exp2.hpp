#ifndef STAN_MATH_PRIM_SCAL_FUN_EXP2_HPP
#define STAN_MATH_PRIM_SCAL_FUN_EXP2_HPP

#include <boost/math/tools/promotion.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the exponent base 2 of the specified argument (C99,
 * C++11).
 *
 * The exponent base 2 function is defined by
 *
 * <code>exp2(y) = pow(2.0, y)</code>.
 *
 * @param y argument.
 * @return exponent base 2 of argument.
 */
inline double exp2(double y) {
  using std::pow;
  return pow(2.0, y);
}

/**
 * Return the exponent base 2 of the specified argument (C99,
 * C++11).
 *
 * @param y argument
 * @return exponent base 2 of argument
 */
inline double exp2(int y) { return exp2(static_cast<double>(y)); }

}  // namespace math
}  // namespace stan
#endif
