#ifndef STAN_MATH_PRIM_SCAL_FUN_PROB_CONSTRAIN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PROB_CONSTRAIN_HPP

#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return a probability value constrained to fall between 0 and 1
 * (inclusive) for the specified free scalar.
 *
 * <p>The transform is the inverse logit,
 *
 * <p>\f$f(x) = \mbox{logit}^{-1}(x) = \frac{1}{1 + \exp(x)}\f$.
 *
 * @tparam T type of scalar
 * @param[in] x unconstrained value
 * @return result constrained to fall in (0, 1)
 */
template <typename T>
inline T prob_constrain(const T& x) {
  return inv_logit(x);
}

/**
 * Return a probability value constrained to fall between 0 and 1
 * (inclusive) for the specified free scalar and increment the
 * specified log probability reference with the log absolute Jacobian
 * determinant of the transform.
 *
 * <p>The transform is as defined for <code>prob_constrain(T)</code>.
 * The log absolute Jacobian determinant is
 *
 * <p>The log absolute Jacobian determinant is
 *
 * <p>\f$\log | \frac{d}{dx} \mbox{logit}^{-1}(x) |\f$
 * <p>\f$\log ((\mbox{logit}^{-1}(x)) (1 - \mbox{logit}^{-1}(x))\f$
 * <p>\f$\log (\mbox{logit}^{-1}(x)) + \log (1 - \mbox{logit}^{-1}(x))\f$.
 *
 * @tparam T type of scalar
 * @param[in] x unconstrained value
 * @param[in, out] lp log density
 * @return result constrained to fall in (0, 1)
 */
template <typename T>
inline T prob_constrain(const T& x, T& lp) {
  using std::log;
  T inv_logit_x = inv_logit(x);
  lp += log(inv_logit_x) + log1m(inv_logit_x);
  return inv_logit_x;
}

}  // namespace math

}  // namespace stan

#endif
