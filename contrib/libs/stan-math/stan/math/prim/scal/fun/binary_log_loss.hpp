#ifndef STAN_MATH_PRIM_SCAL_FUN_BINARY_LOG_LOSS_HPP
#define STAN_MATH_PRIM_SCAL_FUN_BINARY_LOG_LOSS_HPP

#include <stan/math/prim/scal/fun/log1m.hpp>

namespace stan {
namespace math {

/**
 * Returns the log loss function for binary classification
 * with specified reference and response values.
 *
 * The log loss function for prediction \f$\hat{y} \in [0, 1]\f$
 * given outcome \f$y \in \{ 0, 1 \}\f$ is
 *
 * \f$\mbox{logloss}(1, \hat{y}) = -\log \hat{y} \f$, and
 *
 * \f$\mbox{logloss}(0, \hat{y}) = -\log (1 - \hat{y}) \f$.
 *
 * @tparam T value type
 * @param[in] y reference value, either 0 or 1
 * @param[in] y_hat response value in [0, 1]
 * @return Log loss for response given reference value
 */
template <typename T>
inline T binary_log_loss(int y, const T& y_hat) {
  using std::log;
  return y ? -log(y_hat) : -log1m(y_hat);
}

}  // namespace math
}  // namespace stan

#endif
