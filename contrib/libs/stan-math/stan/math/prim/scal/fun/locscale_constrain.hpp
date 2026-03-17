#ifndef STAN_MATH_PRIM_SCAL_FUN_LOCSCALE_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOCSCALE_CONSTRAIN_HPP

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
 * and specified location and scale.
 *
 * <p>The transform applied is
 *
 * <p>\f$f(x) = mu + sigma * x\f$
 *
 * <p>where mu is the location and sigma is the scale.
 *
 * <p>If the location is zero and the scale is one this
 * reduces to <code>identity_constrain(x)</code>.
 *
 * @tparam T type of scalar
 * @tparam M type of mean
 * @tparam S type of scale
 * @param[in] x Unconstrained scalar input
 * @param[in] mu location of constrained output
 * @param[in] sigma scale of constrained output
 * @return linear transformed value correspdonding to inputs
 * @throw std::domain_error if sigma <= 0
 * @throw std::domain_error if mu is not finite
 */
template <typename T, typename M, typename S>
inline typename boost::math::tools::promote_args<T, M, S>::type
locscale_constrain(const T& x, const M& mu, const S& sigma) {
  check_finite("locscale_constrain", "location", mu);
  if (sigma == 1) {
    if (mu == 0)
      return identity_constrain(x);
    return mu + x;
  }
  check_positive_finite("locscale_constrain", "scale", sigma);
  return mu + sigma * x;
}

/**
 * Return the linearly transformed value for the specified unconstrained input
 * and specified location and scale, incrementing the specified
 * reference with the log absolute Jacobian determinant of the
 * transform.
 *
 * <p>The transform applied is
 *
 * <p>\f$f(x) = mu + sigma * x\f$
 *
 * <p>where mu is the location and sigma is the scale.
 *
 * If the location is zero and scale is one, this function
 * reduces to <code>identity_constraint(x, lp)</code>.
 *
 * @tparam T type of scalar
 * @tparam M type of mean
 * @tparam S type of scale
 * @param[in] x Unconstrained scalar input
 * @param[in] mu location of constrained output
 * @param[in] sigma scale of constrained output
 * @param[in,out] lp Reference to log probability to increment.
 * @return linear transformed value corresponding to inputs
 * @throw std::domain_error if sigma <= 0
 * @throw std::domain_error if mu is not finite
 */
template <typename T, typename M, typename S>
inline typename boost::math::tools::promote_args<T, M, S>::type
locscale_constrain(const T& x, const M& mu, const S& sigma, T& lp) {
  using std::log;
  check_finite("locscale_constrain", "location", mu);
  if (sigma == 1) {
    if (mu == 0)
      return identity_constrain(x);
    return mu + x;
  }
  check_positive_finite("locscale_constrain", "scale", sigma);
  lp += size_of(x) * log(sigma);
  return mu + sigma * x;
}

}  // namespace math

}  // namespace stan

#endif
