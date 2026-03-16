#ifndef STAN_MATH_FWD_SCAL_FUN_ATANH_HPP
#define STAN_MATH_FWD_SCAL_FUN_ATANH_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/atanh.hpp>
#include <stan/math/prim/scal/fun/square.hpp>

namespace stan {
namespace math {

/**
 * Return inverse hyperbolic tangent of specified value.
 *
 * @tparam T scalar type of forward-mode autodiff variable
 * argument.
 * @param x Argument.
 * @return Inverse hyperbolic tangent of argument.
 * @throw std::domain_error if x < -1 or x > 1.
 */
template <typename T>
inline fvar<T> atanh(const fvar<T>& x) {
  return fvar<T>(atanh(x.val_), x.d_ / (1 - square(x.val_)));
}

}  // namespace math
}  // namespace stan
#endif
