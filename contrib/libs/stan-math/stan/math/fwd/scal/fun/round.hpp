#ifndef STAN_MATH_FWD_SCAL_FUN_ROUND_HPP
#define STAN_MATH_FWD_SCAL_FUN_ROUND_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/round.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the closest integer to the specified argument, with
 * halfway cases rounded away from zero.
 *
 * The derivative is always zero.
 *
 * @tparam T Scalar type for autodiff variable.
 * @param x Argument.
 * @return The rounded value of the argument.
 */
template <typename T>
inline fvar<T> round(const fvar<T>& x) {
  return fvar<T>(round(x.val_), is_nan(x.val_)
                                    ? std::numeric_limits<double>::quiet_NaN()
                                    : 0.0);
}

}  // namespace math
}  // namespace stan
#endif
