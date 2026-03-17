#ifndef STAN_MATH_PRIM_SCAL_FUN_OFFSET_MULTIPLIER_FREE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_OFFSET_MULTIPLIER_FREE_HPP

#include <stan/math/prim/scal/fun/identity_free.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <boost/math/tools/promotion.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the unconstrained scalar that transforms to the
 * specified offset and multiplier constrained scalar given the specified
 * offset and multiplier.
 *
 * <p>The transfrom in <code>locmultiplier_constrain(T, double, double)</code>,
 * is reversed by the reverse affine transformation,
 *
 * <p>\f$f^{-1}(y) = \frac{y - L}{S}\f$
 *
 * where \f$L\f$ and \f$S\f$ are the offset and multiplier.
 *
 * <p>If the offset is zero and multiplier is one,
 * this function reduces to  <code>identity_free(y)</code>.
 *
 * @tparam T type of scalar
 * @tparam L type of offset
 * @tparam S type of multiplier
 * @param y constrained value
 * @param[in] mu offset of constrained output
 * @param[in] sigma multiplier of constrained output
 * @return the free scalar that transforms to the input scalar
 *   given the offset and multiplier
 * @throw std::domain_error if sigma <= 0
 * @throw std::domain_error if mu is not finite
 */
template <typename T, typename L, typename S>
inline typename boost::math::tools::promote_args<T, L, S>::type
offset_multiplier_free(const T& y, const L& mu, const S& sigma) {
  check_finite("offset_multiplier_free", "offset", mu);
  if (sigma == 1) {
    if (mu == 0)
      return identity_free(y);
    return y - mu;
  }
  check_positive_finite("offset_multiplier_free", "multiplier", sigma);
  return (y - mu) / sigma;
}

}  // namespace math
}  // namespace stan
#endif
