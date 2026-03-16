#ifndef STAN_MATH_PRIM_SCAL_FUN_IS_INTEGER_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IS_INTEGER_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Returns true if the input is an integer and false otherwise.
 *
 * @param x Value to test.
 * @return <code>true</code> if the value is an integer
 */
template <typename T>
inline bool is_integer(T x) {
  return floor(x) == x;
}

}  // namespace math
}  // namespace stan

#endif
