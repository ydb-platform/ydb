#ifndef STAN_MATH_PRIM_SCAL_FUN_SIZE_ZERO_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SIZE_ZERO_HPP

#include <stan/math/prim/scal/meta/length.hpp>
#include <utility>

namespace stan {
namespace math {

/**
 * Returns 1 if input is of length 0, returns 0
 * otherwise
 *
 * @param x argument
 * @return 0 or 1
 */
template <typename T>
inline bool size_zero(T& x) {
  return !length(x);
}

/**
 * Returns 1 if any inputs are of length 0, returns 0
 * otherwise
 *
 * @param x first argument
 * @param xs parameter pack of remaining arguments to forward to function
 * @return 0 or 1
 */
template <typename T, typename... Ts>
inline bool size_zero(T& x, Ts&&... xs) {
  return (size_zero(x) || size_zero(std::forward<Ts>(xs)...));
}
}  // namespace math
}  // namespace stan

#endif
