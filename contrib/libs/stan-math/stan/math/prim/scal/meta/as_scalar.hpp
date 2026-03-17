#ifndef STAN_MATH_PRIM_SCAL_FUN_AS_SCALAR_HPP
#define STAN_MATH_PRIM_SCAL_FUN_AS_SCALAR_HPP

namespace stan {
namespace math {

/**
 * Converts input to a scalar. For scalar arguments this is an identity
 * function.
 * @param a Input value
 * @return Same value
 */
inline double as_scalar(double a) { return a; }

/**
 * Converts input to a scalar. For scalar arguments this is an identity
 * function.
 * @param a Input value
 * @return Same value
 */
inline int as_scalar(int a) { return a; }

}  // namespace math
}  // namespace stan

#endif
