#ifndef STAN_MATH_FWD_SCAL_FUN_LOG1M_EXP_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG1M_EXP_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/scal/fun/expm1.hpp>
#include <stan/math/fwd/scal/fun/log1m_exp.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/expm1.hpp>
#include <stan/math/prim/scal/fun/log1m_exp.hpp>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of one minus the
 * exponentiation of the specified argument.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x Argument.
 * @return log of one minus the exponentiation of the argument.
 */
template <typename T>
inline fvar<T> log1m_exp(const fvar<T>& x) {
  if (x.val_ >= 0)
    return fvar<T>(NOT_A_NUMBER);
  return fvar<T>(log1m_exp(x.val_), x.d_ / -expm1(-x.val_));
}

}  // namespace math
}  // namespace stan
#endif
