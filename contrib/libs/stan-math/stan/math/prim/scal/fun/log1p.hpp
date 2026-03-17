#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG1P_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG1P_HPP

#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of one plus the specified value.
 *
 * \f[
 * \mbox{log1p}(x) = \log(1 + x)
 * \f]
 *
 * This version is more stable for arguments near zero than
 * the direct definition.  If <code>log1p(x)</code> is defined to
 * be negative infinity.
 *
 * @param[in] x Argument.
 * @return Natural log of one plus the argument.
 * @throw std::domain_error If argument is less than -1.
 */
inline double log1p(double x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_greater_or_equal("log1p", "x", x, -1.0);
    return std::log1p(x);
  }
}

/**
 * Return the natural logarithm of one plus the specified
 * argument.  This version is required to disambiguate
 * <code>log1p(int)</code>.
 *
 * @param[in] x Argument.
 * @return Natural logarithm of one plus the argument.
 * @throw std::domain_error If argument is less than -1.
 */
inline double log1p(int x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_greater_or_equal("log1p", "x", x, -1);
    return std::log1p(x);
  }
}

}  // namespace math
}  // namespace stan
#endif
