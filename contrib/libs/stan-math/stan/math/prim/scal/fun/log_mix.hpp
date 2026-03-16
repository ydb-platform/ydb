#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_MIX_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_MIX_HPP

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/log_sum_exp.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the log mixture density with specified mixing proportion
 * and log densities.
 *
 * \f[
 * \mbox{log\_mix}(\theta, \lambda_1, \lambda_2)
 * = \log \left( \theta \lambda_1 + (1 - \theta) \lambda_2 \right).
 * \f]
 *
 * \f[
 * \frac{\partial}{\partial \theta}
 * \mbox{log\_mix}(\theta, \lambda_1, \lambda_2)
 * = FIXME
 * \f]
 *
 * \f[
 * \frac{\partial}{\partial \lambda_1}
 * \mbox{log\_mix}(\theta, \lambda_1, \lambda_2)
 * = FIXME
 * \f]
 *
 * \f[
 * \frac{\partial}{\partial \lambda_2}
 * \mbox{log\_mix}(\theta, \lambda_1, \lambda_2)
 * = FIXME
 * \f]
 *
 * @param[in] theta mixing proportion in [0, 1].
 * @param lambda1 first log density.
 * @param lambda2 second log density.
 * @return log mixture of densities in specified proportion
 */
inline double log_mix(double theta, double lambda1, double lambda2) {
  using std::log;
  check_not_nan("log_mix", "lambda1", lambda1);
  check_not_nan("log_mix", "lambda2", lambda2);
  check_bounded("log_mix", "theta", theta, 0, 1);
  return log_sum_exp(log(theta) + lambda1, log1m(theta) + lambda2);
}

}  // namespace math
}  // namespace stan
#endif
