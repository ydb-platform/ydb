#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG2_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG2_HPP

#include <stan/math/prim/scal/fun/constants.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the base two logarithm of the argument (C99, C++11).
 *
 * The function is defined by:
 *
 * <code>log2(a) = log(a) / std::log(2.0)</code>.
 *
 * @param[in] u argument
 * @return base two logarithm of argument
 */
inline double log2(double u) {
  using std::log;
  return log(u) / LOG_2;
}

/**
 * Return the base two logarithm of the specified argument.  This
 * version is required to disambiguate <code>log2(int)</code>.
 *
 * @param[in] u argument
 * @return base two logarithm of argument
 */
inline double log2(int u) { return log2(static_cast<double>(u)); }

/**
 * Return natural logarithm of two.
 *
 * @return Natural logarithm of two.
 */
inline double log2() { return LOG_2; }

}  // namespace math
}  // namespace stan

#endif
