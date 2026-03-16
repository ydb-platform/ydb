#ifndef STAN_MATH_REV_SCAL_FUN_LOG_SUM_EXP_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG_SUM_EXP_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/rev/scal/fun/calculate_chain.hpp>
#include <stan/math/prim/scal/fun/log_sum_exp.hpp>

namespace stan {
namespace math {

namespace internal {

class log_sum_exp_vv_vari : public op_vv_vari {
 public:
  log_sum_exp_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(log_sum_exp(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    avi_->adj_ += adj_ * calculate_chain(avi_->val_, val_);
    bvi_->adj_ += adj_ * calculate_chain(bvi_->val_, val_);
  }
};
class log_sum_exp_vd_vari : public op_vd_vari {
 public:
  log_sum_exp_vd_vari(vari* avi, double b)
      : op_vd_vari(log_sum_exp(avi->val_, b), avi, b) {}
  void chain() { avi_->adj_ += adj_ * calculate_chain(avi_->val_, val_); }
};
class log_sum_exp_dv_vari : public op_dv_vari {
 public:
  log_sum_exp_dv_vari(double a, vari* bvi)
      : op_dv_vari(log_sum_exp(a, bvi->val_), a, bvi) {}
  void chain() { bvi_->adj_ += adj_ * calculate_chain(bvi_->val_, val_); }
};

}  // namespace internal

/**
 * Returns the log sum of exponentials.
 */
inline var log_sum_exp(const var& a, const var& b) {
  return var(new internal::log_sum_exp_vv_vari(a.vi_, b.vi_));
}
/**
 * Returns the log sum of exponentials.
 */
inline var log_sum_exp(const var& a, double b) {
  return var(new internal::log_sum_exp_vd_vari(a.vi_, b));
}
/**
 * Returns the log sum of exponentials.
 */
inline var log_sum_exp(double a, const var& b) {
  return var(new internal::log_sum_exp_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
