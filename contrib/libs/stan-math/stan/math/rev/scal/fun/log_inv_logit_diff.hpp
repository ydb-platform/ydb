#ifndef STAN_MATH_REV_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit_diff.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/inv.hpp>
#include <stan/math/prim/scal/fun/expm1.hpp>

namespace stan {
namespace math {

/*
 * Returns the natural logarithm of the difference of the
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
 * @tparam T1 Type of x argument
 * @tparam T2 Type of y argument
 * @param a Argument
 * @param b Argument
 * @return Result of log difference of inverse logits of arguments
 *          and gradients
 */
namespace internal {
class log_inv_logit_diff_vv_vari : public op_vv_vari {
 public:
  log_inv_logit_diff_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(log_inv_logit_diff(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    avi_->adj_
        -= adj_ * (inv(expm1(bvi_->val_ - avi_->val_)) + inv_logit(avi_->val_));

    bvi_->adj_
        -= adj_ * (inv(expm1(avi_->val_ - bvi_->val_)) + inv_logit(bvi_->val_));
  }
};

class log_inv_logit_diff_vd_vari : public op_vd_vari {
 public:
  log_inv_logit_diff_vd_vari(vari* avi, double b)
      : op_vd_vari(log_inv_logit_diff(avi->val_, b), avi, b) {}
  void chain() {
    avi_->adj_ -= adj_ * (inv(expm1(bd_ - avi_->val_)) + inv_logit(avi_->val_));
  }
};

class log_inv_logit_diff_dv_vari : public op_dv_vari {
 public:
  log_inv_logit_diff_dv_vari(double a, vari* bvi)
      : op_dv_vari(log_inv_logit_diff(a, bvi->val_), a, bvi) {}
  void chain() {
    bvi_->adj_ -= adj_ * (inv(expm1(ad_ - bvi_->val_)) + inv_logit(bvi_->val_));
  }
};
}  // namespace internal

inline var log_inv_logit_diff(const var& a, double b) {
  return var(new internal::log_inv_logit_diff_vd_vari(a.vi_, b));
}

inline var log_inv_logit_diff(const var& a, const var& b) {
  return var(new internal::log_inv_logit_diff_vv_vari(a.vi_, b.vi_));
}

inline var log_inv_logit_diff(double a, const var& b) {
  return var(new internal::log_inv_logit_diff_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
