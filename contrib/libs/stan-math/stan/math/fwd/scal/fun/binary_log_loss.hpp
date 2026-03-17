#ifndef STAN_MATH_FWD_SCAL_FUN_BINARY_LOG_LOSS_HPP
#define STAN_MATH_FWD_SCAL_FUN_BINARY_LOG_LOSS_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/binary_log_loss.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> binary_log_loss(int y, const fvar<T>& y_hat) {
  if (y)
    return fvar<T>(binary_log_loss(y, y_hat.val_), -y_hat.d_ / y_hat.val_);
  else
    return fvar<T>(binary_log_loss(y, y_hat.val_),
                   y_hat.d_ / (1.0 - y_hat.val_));
}
}  // namespace math
}  // namespace stan
#endif
