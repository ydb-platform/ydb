#ifndef STAN_MATH_REV_CORE_OPERATOR_GREATER_THAN_OR_EQUAL_HPP
#define STAN_MATH_REV_CORE_OPERATOR_GREATER_THAN_OR_EQUAL_HPP

#include <stan/math/rev/core/var.hpp>

namespace stan {
namespace math {

/**
 * Greater than or equal operator comparing two variables' values
 * (C++).
 *
   \f[
   \mbox{operator\textgreater=}(x, y) =
   \begin{cases}
     0 & \mbox{if } x < y\\
     1 & \mbox{if } x \geq y \\[6pt]
     0 & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a First variable.
 * @param b Second variable.
 * @return True if first variable's value is greater than or equal
 * to the second's.
 */
inline bool operator>=(const var& a, const var& b) {
  return a.val() >= b.val();
}

/**
 * Greater than or equal operator comparing variable's value and
 * double (C++).
 *
 * @param a First variable.
 * @param b Second value.
 * @return True if first variable's value is greater than or equal
 * to second value.
 */
inline bool operator>=(const var& a, double b) { return a.val() >= b; }

/**
 * Greater than or equal operator comparing double and variable's
 * value (C++).
 *
 * @param a First value.
 * @param b Second variable.
 * @return True if the first value is greater than or equal to the
 * second variable's value.
 */
inline bool operator>=(double a, const var& b) { return a >= b.val(); }

}  // namespace math
}  // namespace stan
#endif
