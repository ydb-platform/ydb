#ifndef STAN_MATH_REV_SCAL_FUN_LOG_RISING_FACTORIAL_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG_RISING_FACTORIAL_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/log_rising_factorial.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/rev/scal/fun/digamma.hpp>

namespace stan {
namespace math {

namespace internal {

class log_rising_factorial_vv_vari : public op_vv_vari {
 public:
  log_rising_factorial_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(log_rising_factorial(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    avi_->adj_
        += adj_ * (digamma(avi_->val_ + bvi_->val_) - digamma(avi_->val_));
    bvi_->adj_ += adj_ * digamma(avi_->val_ + bvi_->val_);
  }
};

class log_rising_factorial_vd_vari : public op_vd_vari {
 public:
  log_rising_factorial_vd_vari(vari* avi, double b)
      : op_vd_vari(log_rising_factorial(avi->val_, b), avi, b) {}
  void chain() {
    avi_->adj_ += adj_ * (digamma(avi_->val_ + bd_) - digamma(avi_->val_));
  }
};

class log_rising_factorial_dv_vari : public op_dv_vari {
 public:
  log_rising_factorial_dv_vari(double a, vari* bvi)
      : op_dv_vari(log_rising_factorial(a, bvi->val_), a, bvi) {}
  void chain() { bvi_->adj_ += adj_ * digamma(bvi_->val_ + ad_); }
};
}  // namespace internal

inline var log_rising_factorial(const var& a, double b) {
  return var(new internal::log_rising_factorial_vd_vari(a.vi_, b));
}

inline var log_rising_factorial(const var& a, const var& b) {
  return var(new internal::log_rising_factorial_vv_vari(a.vi_, b.vi_));
}

inline var log_rising_factorial(double a, const var& b) {
  return var(new internal::log_rising_factorial_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
