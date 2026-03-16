#ifndef STAN_MATH_REV_SCAL_FUN_TO_VAR_HPP
#define STAN_MATH_REV_SCAL_FUN_TO_VAR_HPP

#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

/**
 * Converts argument to an automatic differentiation variable.
 *
 * Returns a var variable with the input value.
 *
 * @param[in] x A scalar value
 * @return An automatic differentiation variable with the input value.
 */
inline var to_var(double x) { return var(x); }

/**
 * Converts argument to an automatic differentiation variable.
 *
 * Returns a var variable with the input value.
 *
 * @param[in] x An automatic differentiation variable.
 * @return An automatic differentiation variable with the input value.
 */
inline var to_var(const var& x) { return x; }

}  // namespace math
}  // namespace stan
#endif
