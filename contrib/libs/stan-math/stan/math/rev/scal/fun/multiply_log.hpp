#ifndef STAN_MATH_REV_SCAL_FUN_MULTIPLY_LOG_HPP
#define STAN_MATH_REV_SCAL_FUN_MULTIPLY_LOG_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/rev/scal/fun/log.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class multiply_log_vv_vari : public op_vv_vari {
 public:
  multiply_log_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(multiply_log(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    using std::log;
    if (unlikely(is_nan(avi_->val_) || is_nan(bvi_->val_))) {
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
      bvi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    } else {
      avi_->adj_ += adj_ * log(bvi_->val_);
      if (bvi_->val_ == 0.0 && avi_->val_ == 0)
        bvi_->adj_ += adj_ * std::numeric_limits<double>::infinity();
      else
        bvi_->adj_ += adj_ * avi_->val_ / bvi_->val_;
    }
  }
};
class multiply_log_vd_vari : public op_vd_vari {
 public:
  multiply_log_vd_vari(vari* avi, double b)
      : op_vd_vari(multiply_log(avi->val_, b), avi, b) {}
  void chain() {
    using std::log;
    if (unlikely(is_nan(avi_->val_) || is_nan(bd_)))
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    else
      avi_->adj_ += adj_ * log(bd_);
  }
};
class multiply_log_dv_vari : public op_dv_vari {
 public:
  multiply_log_dv_vari(double a, vari* bvi)
      : op_dv_vari(multiply_log(a, bvi->val_), a, bvi) {}
  void chain() {
    if (bvi_->val_ == 0.0 && ad_ == 0.0)
      bvi_->adj_ += adj_ * std::numeric_limits<double>::infinity();
    else
      bvi_->adj_ += adj_ * ad_ / bvi_->val_;
  }
};
}  // namespace internal

/**
 * Return the value of a*log(b).
 *
 * When both a and b are 0, the value returned is 0.
 * The partial deriviative with respect to a is log(b).
 * The partial deriviative with respect to b is a/b. When
 * a and b are both 0, this is set to Inf.
 *
 * @param a First variable.
 * @param b Second variable.
 * @return Value of a*log(b)
 */
inline var multiply_log(const var& a, const var& b) {
  return var(new internal::multiply_log_vv_vari(a.vi_, b.vi_));
}
/**
 * Return the value of a*log(b).
 *
 * When both a and b are 0, the value returned is 0.
 * The partial deriviative with respect to a is log(b).
 *
 * @param a First variable.
 * @param b Second scalar.
 * @return Value of a*log(b)
 */
inline var multiply_log(const var& a, double b) {
  return var(new internal::multiply_log_vd_vari(a.vi_, b));
}
/**
 * Return the value of a*log(b).
 *
 * When both a and b are 0, the value returned is 0.
 * The partial deriviative with respect to b is a/b. When
 * a and b are both 0, this is set to Inf.
 *
 * @param a First scalar.
 * @param b Second variable.
 * @return Value of a*log(b)
 */
inline var multiply_log(double a, const var& b) {
  if (a == 1.0)
    return log(b);
  return var(new internal::multiply_log_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
