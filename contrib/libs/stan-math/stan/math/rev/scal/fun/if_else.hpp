#ifndef STAN_MATH_REV_SCAL_FUN_IF_ELSE_HPP
#define STAN_MATH_REV_SCAL_FUN_IF_ELSE_HPP

#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

/**
 * If the specified condition is true, return the first
 * variable, otherwise return the second variable.
 *
 * @param c Boolean condition.
 * @param y_true Variable to return if condition is true.
 * @param y_false Variable to return if condition is false.
 */
inline var if_else(bool c, const var& y_true, const var& y_false) {
  return c ? y_true : y_false;
}
/**
 * If the specified condition is true, return a new variable
 * constructed from the first scalar, otherwise return the second
 * variable.
 *
 * @param c Boolean condition.
 * @param y_true Value to promote to variable and return if condition is true.
 * @param y_false Variable to return if condition is false.
 */
inline var if_else(bool c, double y_true, const var& y_false) {
  if (c)
    return var(y_true);
  else
    return y_false;
}
/**
 * If the specified condition is true, return the first variable,
 * otherwise return a new variable constructed from the second
 * scalar.
 *
 * @param c Boolean condition.
 * @param y_true Variable to return if condition is true.
 * @param y_false Value to promote to variable and return if condition is false.
 */
inline var if_else(bool c, const var& y_true, double y_false) {
  if (c)
    return y_true;
  else
    return var(y_false);
}

}  // namespace math
}  // namespace stan
#endif
