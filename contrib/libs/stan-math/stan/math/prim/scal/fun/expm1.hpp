#ifndef STAN_MATH_PRIM_SCAL_FUN_EXPM1_HPP
#define STAN_MATH_PRIM_SCAL_FUN_EXPM1_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the natural exponentiation of x minus one.
 * Returns infinity for infinity argument and -infinity for
 * -infinity argument.
 *
 * @param[in] x Argument.
 * @return Natural exponentiation of argument minus one.
 */
inline double expm1(double x) { return std::expm1(x); }

/**
 * Integer version of expm1.
 *
 * @param[in] x Argument.
 * @return Natural exponentiation of argument minus one.
 */
inline double expm1(int x) { return std::expm1(x); }

}  // namespace math
}  // namespace stan
#endif
