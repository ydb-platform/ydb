#ifndef STAN_MATH_PRIM_SCAL_FUN_ACOSH_HPP
#define STAN_MATH_PRIM_SCAL_FUN_ACOSH_HPP

#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the inverse hyperbolic cosine of the specified value.
 * Returns nan for nan argument.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic cosine of the argument.
 * @throw std::domain_error If argument is less than 1.
 */
inline double acosh(double x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_greater_or_equal("acosh", "x", x, 1.0);
#ifdef _WIN32
    if (is_inf(x))
      return x;
#endif
    return std::acosh(x);
  }
}

/**
 * Integer version of acosh.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic cosine of the argument.
 * @throw std::domain_error If argument is less than 1.
 */
inline double acosh(int x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_greater_or_equal("acosh", "x", x, 1);
#ifdef _WIN32
    if (is_inf(x))
      return x;
#endif
    return std::acosh(x);
  }
}

}  // namespace math
}  // namespace stan
#endif
