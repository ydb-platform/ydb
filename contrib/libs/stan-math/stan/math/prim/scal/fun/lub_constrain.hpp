#ifndef STAN_MATH_PRIM_SCAL_FUN_LUB_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LUB_CONSTRAIN_HPP

#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/fun/lb_constrain.hpp>
#include <stan/math/prim/scal/fun/ub_constrain.hpp>
#include <boost/math/tools/promotion.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the lower- and upper-bounded scalar derived by
 * transforming the specified free scalar given the specified
 * lower and upper bounds.
 *
 * <p>The transform is the transformed and scaled inverse logit,
 *
 * <p>\f$f(x) = L + (U - L) \mbox{logit}^{-1}(x)\f$
 *
 * If the lower bound is negative infinity and upper bound finite,
 * this function reduces to <code>ub_constrain(x, ub)</code>.  If
 * the upper bound is positive infinity and the lower bound
 * finite, this function reduces to
 * <code>lb_constrain(x, lb)</code>.  If the upper bound is
 * positive infinity and the lower bound negative infinity,
 * this function reduces to <code>identity_constrain(x)</code>.
 *
 * @tparam T Type of scalar.
 * @tparam L Type of lower bound.
 * @tparam U Type of upper bound.
 * @param[in] x Free scalar to transform.
 * @param[in] lb Lower bound.
 * @param[in] ub Upper bound.
 * @return Lower- and upper-bounded scalar derived from transforming
 *   the free scalar.
 * @throw std::domain_error if ub <= lb
 */
template <typename T, typename L, typename U>
inline typename boost::math::tools::promote_args<T, L, U>::type lub_constrain(
    const T& x, const L& lb, const U& ub) {
  using std::exp;
  check_less("lub_constrain", "lb", lb, ub);
  if (lb == -std::numeric_limits<double>::infinity())
    return ub_constrain(x, ub);
  if (ub == std::numeric_limits<double>::infinity())
    return lb_constrain(x, lb);

  T inv_logit_x;
  if (x > 0) {
    T exp_minus_x = exp(-x);
    inv_logit_x = 1.0 / (1.0 + exp_minus_x);
    // Prevent x from reaching one unless it really really should.
    if ((x < std::numeric_limits<double>::infinity()) && (inv_logit_x == 1))
      inv_logit_x = 1 - 1e-15;
  } else {
    T exp_x = exp(x);
    inv_logit_x = 1.0 - 1.0 / (1.0 + exp_x);
    // Prevent x from reaching zero unless it really really should.
    if ((x > -std::numeric_limits<double>::infinity()) && (inv_logit_x == 0))
      inv_logit_x = 1e-15;
  }
  return lb + (ub - lb) * inv_logit_x;
}

/**
 * Return the lower- and upper-bounded scalar derived by
 * transforming the specified free scalar given the specified
 * lower and upper bounds and increment the specified log
 * probability with the log absolute Jacobian determinant.
 *
 * <p>The transform is as defined in
 * <code>lub_constrain(T, double, double)</code>.  The log absolute
 * Jacobian determinant is given by
 *
 * <p>\f$\log \left| \frac{d}{dx} \left(
 *                L + (U-L) \mbox{logit}^{-1}(x) \right)
 *            \right|\f$
 *
 * <p>\f$ {} = \log |
 *         (U-L)
 *         \, (\mbox{logit}^{-1}(x))
 *         \, (1 - \mbox{logit}^{-1}(x)) |\f$
 *
 * <p>\f$ {} = \log (U - L) + \log (\mbox{logit}^{-1}(x))
 *                          + \log (1 - \mbox{logit}^{-1}(x))\f$
 *
 * <p>If the lower bound is negative infinity and upper bound finite,
 * this function reduces to <code>ub_constrain(x, ub, lp)</code>.  If
 * the upper bound is positive infinity and the lower bound
 * finite, this function reduces to
 * <code>lb_constrain(x, lb, lp)</code>.  If the upper bound is
 * positive infinity and the lower bound negative infinity,
 * this function reduces to <code>identity_constrain(x, lp)</code>.
 *
 * @tparam T Type of scalar.
 * @tparam L Type of lower bound.
 * @tparam U Type of upper bound.
 * @param[in] x Free scalar to transform.
 * @param[in] lb Lower bound.
 * @param[in] ub Upper bound.
 * @param[in,out] lp Log probability scalar reference.
 * @return Lower- and upper-bounded scalar derived from transforming
 *   the free scalar.
 * @throw std::domain_error if ub <= lb
 */
template <typename T, typename L, typename U>
inline typename boost::math::tools::promote_args<T, L, U>::type lub_constrain(
    const T& x, const L& lb, const U& ub, T& lp) {
  using std::exp;
  using std::log;
  check_less("lub_constrain", "lb", lb, ub);
  if (lb == -std::numeric_limits<double>::infinity())
    return ub_constrain(x, ub, lp);
  if (ub == std::numeric_limits<double>::infinity())
    return lb_constrain(x, lb, lp);
  T inv_logit_x;
  if (x > 0) {
    T exp_minus_x = exp(-x);
    inv_logit_x = 1.0 / (1.0 + exp_minus_x);
    lp += log(ub - lb) - x - 2 * log1p(exp_minus_x);
    // Prevent x from reaching one unless it really really should.
    if ((x < std::numeric_limits<double>::infinity()) && (inv_logit_x == 1))
      inv_logit_x = 1 - 1e-15;
  } else {
    T exp_x = exp(x);
    inv_logit_x = 1.0 - 1.0 / (1.0 + exp_x);
    lp += log(ub - lb) + x - 2 * log1p(exp_x);
    // Prevent x from reaching zero unless it really really should.
    if ((x > -std::numeric_limits<double>::infinity()) && (inv_logit_x == 0))
      inv_logit_x = 1e-15;
  }
  return lb + (ub - lb) * inv_logit_x;
}

}  // namespace math
}  // namespace stan
#endif
