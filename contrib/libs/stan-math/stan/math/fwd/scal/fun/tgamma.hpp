#ifndef STAN_MATH_FWD_SCAL_FUN_TGAMMA_HPP
#define STAN_MATH_FWD_SCAL_FUN_TGAMMA_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/tgamma.hpp>
#include <boost/math/special_functions/digamma.hpp>

namespace stan {
namespace math {

/**
 * Return the result of applying the gamma function to the
 * specified argument.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x Argument.
 * @return Gamma function applied to argument.
 */
template <typename T>
inline fvar<T> tgamma(const fvar<T>& x) {
  using boost::math::digamma;
  T u = tgamma(x.val_);
  return fvar<T>(u, x.d_ * u * digamma(x.val_));
}
}  // namespace math
}  // namespace stan
#endif
