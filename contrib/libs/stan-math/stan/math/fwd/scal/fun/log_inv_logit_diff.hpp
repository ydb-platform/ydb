#ifndef STAN_MATH_FWD_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit_diff.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/inv.hpp>
#include <stan/math/prim/scal/fun/expm1.hpp>

namespace stan {
namespace math {

/**
 * Returns fvar with the natural logarithm of the difference of the
 * inverse logits of the specified arguments and its gradients.
 *
   \f[
     \mathrm{log\_inv\_logit\_diff}(x,y) =
      \ln\left(\frac{1}{1+\exp(-x)}-\frac{1}{1+\exp(-y)}\right)
   \f]

   \f[
    \frac{\partial }{\partial x} = -\frac{e^x}{e^y-e^x}-\frac{e^x}{e^x+1}
   \f]

   \f[
    \frac{\partial }{\partial x} = -\frac{e^y}{e^x-e^y}-\frac{e^y}{e^y+1}
   \f]
 *
 * @tparam T1 Type of x argument.
 * @tparam T2 Type of y argument.
 * @param x Argument.
 * @param y Argument.
 * @return Fvar with result of log difference of inverse logits of arguments
 *           and gradients.
 */
template <typename T>
inline fvar<T> log_inv_logit_diff(const fvar<T>& x, const fvar<T>& y) {
  return fvar<T>(
      log_inv_logit_diff(x.val_, y.val_),
      -x.d_ * (inv(expm1(y.val_ - x.val_)) + inv_logit(x.val_))
          - y.d_ * (inv(expm1(x.val_ - y.val_)) + inv_logit(y.val_)));
}

template <typename T>
inline fvar<T> log_inv_logit_diff(const fvar<T>& x, double y) {
  return fvar<T>(log_inv_logit_diff(x.val_, y),
                 -x.d_ * (inv(expm1(y - x.val_)) + inv_logit(x.val_)));
}

template <typename T>
inline fvar<T> log_inv_logit_diff(double x, const fvar<T>& y) {
  return fvar<T>(log_inv_logit_diff(x, y.val_),
                 -y.d_ * (inv(expm1(x - y.val_)) + inv_logit(y.val_)));
}

}  // namespace math
}  // namespace stan
#endif
