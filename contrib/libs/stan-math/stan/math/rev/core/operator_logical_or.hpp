#ifndef STAN_MATH_REV_CORE_OPERATOR_LOGICAL_OR_HPP
#define STAN_MATH_REV_CORE_OPERATOR_LOGICAL_OR_HPP

#include <stan/math/rev/core/var.hpp>

namespace stan {
namespace math {

/**
 * Return the logical disjunction of the values of the two
 * arguments as defined by <code>||</code>.
 *
 * @param[in] x first argument
 * @param[in] y second argument
 * @return disjuntion of the argument's values
 */
inline bool operator||(const var& x, const var& y) {
  return x.val() || y.val();
}

/**
 * Return the logical disjunction of the values of the two
 * arguments as defined by <code>||</code>.
 *
 * @param[in] x first argument
 * @param[in] y second argument
 * @return disjunction of first argument's value and second
 * argument
 */
template <typename T>
inline bool operator||(const var& x, double y) {
  return x.val() || y;
}

/**
 * Return the logical disjunction of the values of the two
 * arguments as defined by <code>||</code>.
 *
 * @param[in] x first argument
 * @param[in] y second argument
 * @return disjunction of first argument and the second
 * argument's value
 */
template <typename T>
inline bool operator||(double x, const var& y) {
  return x || y.val();
}

}  // namespace math
}  // namespace stan
#endif
