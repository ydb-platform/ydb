#ifndef STAN_MATH_PRIM_SCAL_FUN_CORR_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_CORR_CONSTRAIN_HPP

#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the result of transforming the specified scalar to have
 * a valid correlation value between -1 and 1 (inclusive).
 *
 * <p>The transform used is the hyperbolic tangent function,
 *
 * <p>\f$f(x) = \tanh x = \frac{\exp(2x) - 1}{\exp(2x) + 1}\f$.
 *
 * @tparam T type of value
 * @param[in] x value
 * @return tanh transform of value
 */
template <typename T>
inline T corr_constrain(const T& x) {
  return tanh(x);
}

/**
 * Return the result of transforming the specified scalar to have
 * a valid correlation value between -1 and 1 (inclusive).
 *
 * <p>The transform used is as specified for
 * <code>corr_constrain(T)</code>.  The log absolute Jacobian
 * determinant is
 *
 * <p>\f$\log | \frac{d}{dx} \tanh x  | = \log (1 - \tanh^2 x)\f$.
 *
 * @tparam T Type of scalar.
 * @param[in] x value
 * @param[in,out] lp log density accumulator
 */
template <typename T>
inline T corr_constrain(const T& x, T& lp) {
  T tanh_x = tanh(x);
  lp += log1m(square(tanh_x));
  return tanh_x;
}

}  // namespace math
}  // namespace stan
#endif
