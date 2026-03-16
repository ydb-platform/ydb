#ifndef STAN_MATH_REV_CORE_OPERATOR_EQUAL_HPP
#define STAN_MATH_REV_CORE_OPERATOR_EQUAL_HPP

#include <stan/math/rev/core/var.hpp>

namespace stan {
namespace math {

/**
 * Equality operator comparing two variables' values (C++).
 *
   \f[
   \mbox{operator==}(x, y) =
   \begin{cases}
     0 & \mbox{if } x \neq y\\
     1 & \mbox{if } x = y \\[6pt]
     0 & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a First variable.
 * @param b Second variable.
 * @return True if the first variable's value is the same as the
 * second's.
 */
inline bool operator==(const var& a, const var& b) {
  return a.val() == b.val();
}

/**
 * Equality operator comparing a variable's value and a double
 * (C++).
 *
 * @param a First variable.
 * @param b Second value.
 * @return True if the first variable's value is the same as the
 * second value.
 */
inline bool operator==(const var& a, double b) { return a.val() == b; }

/**
 * Equality operator comparing a scalar and a variable's value
 * (C++).
 *
 * @param a First scalar.
 * @param b Second variable.
 * @return True if the variable's value is equal to the scalar.
 */
inline bool operator==(double a, const var& b) { return a == b.val(); }

}  // namespace math
}  // namespace stan
#endif
