#ifndef STAN_MATH_PRIM_SCAL_FUN_LB_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LB_CONSTRAIN_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/scal/fun/identity_constrain.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the lower-bounded value for the specified unconstrained input
 * and specified lower bound.
 *
 * <p>The transform applied is
 *
 * <p>\f$f(x) = \exp(x) + L\f$
 *
 * <p>where \f$L\f$ is the constant lower bound.
 *
 * <p>If the lower bound is negative infinity, this function
 * reduces to <code>identity_constrain(x)</code>.
 *
 * @tparam T type of scalar
 * @tparam L type of lower bound
 * @param[in] x Unconstrained scalar input
 * @param[in] lb lower bound on constrained ouptut
 * @return lower bound constrained value correspdonding to inputs
 */
template <typename T, typename L>
inline typename boost::math::tools::promote_args<T, L>::type lb_constrain(
    const T& x, const L& lb) {
  using std::exp;
  if (lb == -std::numeric_limits<double>::infinity())
    return identity_constrain(x);
  return exp(x) + lb;
}

/**
 * Return the lower-bounded value for the speicifed unconstrained
 * input and specified lower bound, incrementing the specified
 * reference with the log absolute Jacobian determinant of the
 * transform.
 *
 * If the lower bound is negative infinity, this function
 * reduces to <code>identity_constraint(x, lp)</code>.
 *
 * @tparam T type of scalar.
 * @tparam L type of lower bound.
 * @param[in] x unconstrained scalar input
 * @param[in] lb lower bound on output
 * @param[in,out] lp Reference to log probability to increment.
 * @return lower-bound constrained value corresponding to inputs
 */
template <typename T, typename L>
inline typename boost::math::tools::promote_args<T, L>::type lb_constrain(
    const T& x, const L& lb, T& lp) {
  using std::exp;
  if (lb == -std::numeric_limits<double>::infinity())
    return identity_constrain(x, lp);
  lp += x;
  return exp(x) + lb;
}

}  // namespace math

}  // namespace stan

#endif
