#ifndef STAN_MATH_REV_SCAL_FUN_BINARY_LOG_LOSS_HPP
#define STAN_MATH_REV_SCAL_FUN_BINARY_LOG_LOSS_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/log1p.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class binary_log_loss_1_vari : public op_v_vari {
 public:
  explicit binary_log_loss_1_vari(vari* avi)
      : op_v_vari(-std::log(avi->val_), avi) {}
  void chain() { avi_->adj_ -= adj_ / avi_->val_; }
};

class binary_log_loss_0_vari : public op_v_vari {
 public:
  explicit binary_log_loss_0_vari(vari* avi)
      : op_v_vari(-log1p(-avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ / (1.0 - avi_->val_); }
};
}  // namespace internal

/**
 * The log loss function for variables (stan).
 *
 * See binary_log_loss() for the double-based version.
 *
 * The derivative with respect to the variable \f$\hat{y}\f$ is
 *
 * \f$\frac{d}{d\hat{y}} \mbox{logloss}(1, \hat{y}) = - \frac{1}{\hat{y}}\f$,
 and
 *
 * \f$\frac{d}{d\hat{y}} \mbox{logloss}(0, \hat{y}) = \frac{1}{1 - \hat{y}}\f$.
 *
 *
   \f[
   \mbox{binary\_log\_loss}(y, \hat{y}) =
   \begin{cases}
     y \log \hat{y} + (1 - y) \log (1 - \hat{y}) & \mbox{if } 0\leq \hat{y}\leq
 1, y\in\{ 0, 1 \}\\[6pt] \textrm{NaN} & \mbox{if } \hat{y} = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{binary\_log\_loss}(y, \hat{y})}{\partial \hat{y}} =
   \begin{cases}
     \frac{y}{\hat{y}}-\frac{1-y}{1-\hat{y}} & \mbox{if } 0\leq \hat{y}\leq 1,
     y\in\{ 0, 1 \}\\[6pt]
     \textrm{NaN} & \mbox{if } \hat{y} = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param y Reference value.
 * @param y_hat Response variable.
 * @return Log loss of response versus reference value.
 */
inline var binary_log_loss(int y, const var& y_hat) {
  if (y == 0)
    return var(new internal::binary_log_loss_0_vari(y_hat.vi_));
  else
    return var(new internal::binary_log_loss_1_vari(y_hat.vi_));
}

}  // namespace math
}  // namespace stan
#endif
