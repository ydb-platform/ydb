#ifndef STAN_MATH_PRIM_SCAL_FUN_IF_ELSE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IF_ELSE_HPP

#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Return the second argument if the first argument is true
 * and otherwise return the second argument.
 *
 * <p>This is just a convenience method to provide a function
 * with the same behavior as the built-in ternary operator.
 * In general, this function behaves as if defined by
 *
 * <p><code>if_else(c, y1, y0) = c ? y1 : y0</code>.
 *
 * @param c Boolean condition value.
 * @param y_true Value to return if condition is true.
 * @param y_false Value to return if condition is false.
 */
template <typename T_true, typename T_false>
inline typename boost::math::tools::promote_args<T_true, T_false>::type if_else(
    const bool c, const T_true y_true, const T_false y_false) {
  return c ? y_true : y_false;
}

}  // namespace math
}  // namespace stan

#endif
