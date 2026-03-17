#ifndef STAN_MATH_PRIM_SCAL_FUN_ATANH_HPP
#define STAN_MATH_PRIM_SCAL_FUN_ATANH_HPP

#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the inverse hyperbolic tangent of the specified value.
 * An argument of -1 returns negative infinity and an argument of 1
 * returns infinity.
 * Returns nan for nan argument.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic tangent of the argument.
 * @throw std::domain_error If argument is not in [-1, 1].
 */
inline double atanh(double x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_bounded("atanh", "x", x, -1.0, 1.0);
    return std::atanh(x);
  }
}

/**
 * Integer version of atanh.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic tangent of the argument.
 * @throw std::domain_error If argument is less than 1.
 */
inline double atanh(int x) {
  if (is_nan(x)) {
    return x;
  } else {
    check_bounded("atanh", "x", x, -1, 1);
    return std::atanh(x);
  }
}

}  // namespace math
}  // namespace stan
#endif
