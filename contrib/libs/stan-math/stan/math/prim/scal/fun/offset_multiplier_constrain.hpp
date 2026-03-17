#ifndef STAN_MATH_PRIM_SCAL_FUN_OFFSET_MULTIPLIER_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_OFFSET_MULTIPLIER_CONSTRAIN_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/scal/fun/identity_constrain.hpp>
#include <stan/math/prim/scal/fun/abs.hpp>
#include <stan/math/prim/scal/meta/size_of.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the linearly transformed value for the specified unconstrained input
 * and specified offset and multiplier.
 *
 * <p>The transform applied is
 *
 * <p>\f$f(x) = mu + sigma * x\f$
 *
 * <p>where mu is the offset and sigma is the multiplier.
 *
 * <p>If the offset is zero and the multiplier is one this
 * reduces to <code>identity_constrain(x)</code>.
 *
 * @tparam T type of scalar
 * @tparam M type of offset
 * @tparam S type of multiplier
 * @param[in] x Unconstrained scalar input
 * @param[in] mu offset of constrained output
 * @param[in] sigma multiplier of constrained output
 * @return linear transformed value correspdonding to inputs
 * @throw std::domain_error if sigma <= 0
 * @throw std::domain_error if mu is not finite
 */
template <typename T, typename M, typename S>
inline typename boost::math::tools::promote_args<T, M, S>::type
offset_multiplier_constrain(const T& x, const M& mu, const S& sigma) {
  check_finite("offset_multiplier_constrain", "offset", mu);
  if (sigma == 1) {
    if (mu == 0)
      return identity_constrain(x);
    return mu + x;
  }
  check_positive_finite("offset_multiplier_constrain", "multiplier", sigma);
  return mu + sigma * x;
}

/**
 * Return the linearly transformed value for the specified unconstrained input
 * and specified offset and multiplier, incrementing the specified
 * reference with the log absolute Jacobian determinant of the
 * transform.
 *
 * <p>The transform applied is
 *
 * <p>\f$f(x) = mu + sigma * x\f$
 *
 * <p>where mu is the offset and sigma is the multiplier.
 *
 * If the offset is zero and multiplier is one, this function
 * reduces to <code>identity_constraint(x, lp)</code>.
 *
 * @tparam T type of scalar
 * @tparam M type of offset
 * @tparam S type of multiplier
 * @param[in] x Unconstrained scalar input
 * @param[in] mu offset of constrained output
 * @param[in] sigma multiplier of constrained output
 * @param[in,out] lp Reference to log probability to increment.
 * @return linear transformed value corresponding to inputs
 * @throw std::domain_error if sigma <= 0
 * @throw std::domain_error if mu is not finite
 */
template <typename T, typename M, typename S>
inline typename boost::math::tools::promote_args<T, M, S>::type
offset_multiplier_constrain(const T& x, const M& mu, const S& sigma, T& lp) {
  using std::log;
  check_finite("offset_multiplier_constrain", "offset", mu);
  if (sigma == 1) {
    if (mu == 0)
      return identity_constrain(x);
    return mu + x;
  }
  check_positive_finite("offset_multiplier_constrain", "multiplier", sigma);
  lp += size_of(x) * log(sigma);
  return mu + sigma * x;
}

}  // namespace math

}  // namespace stan

#endif
