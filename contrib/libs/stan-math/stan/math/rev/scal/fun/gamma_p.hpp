#ifndef STAN_MATH_REV_SCAL_FUN_GAMMA_P_HPP
#define STAN_MATH_REV_SCAL_FUN_GAMMA_P_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/gamma_p.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/grad_reg_lower_inc_gamma.hpp>
#include <valarray>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class gamma_p_vv_vari : public op_vv_vari {
 public:
  gamma_p_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(gamma_p(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    using std::exp;
    using std::fabs;
    using std::log;

    if (is_inf(avi_->val_)) {
      avi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      bvi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }
    if (is_inf(bvi_->val_)) {
      avi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      bvi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }

    // return zero derivative as gamma_p is flat
    // to machine precision for b / a > 10
    if (std::fabs(bvi_->val_ / avi_->val_) > 10)
      return;

    avi_->adj_ += adj_ * grad_reg_lower_inc_gamma(avi_->val_, bvi_->val_);
    bvi_->adj_
        += adj_
           * std::exp(-bvi_->val_ + (avi_->val_ - 1.0) * std::log(bvi_->val_)
                      - lgamma(avi_->val_));
  }
};

class gamma_p_vd_vari : public op_vd_vari {
 public:
  gamma_p_vd_vari(vari* avi, double b)
      : op_vd_vari(gamma_p(avi->val_, b), avi, b) {}
  void chain() {
    if (is_inf(avi_->val_)) {
      avi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }
    if (is_inf(bd_)) {
      avi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }

    // return zero derivative as gamma_p is flat
    // to machine precision for b / a > 10
    if (std::fabs(bd_ / avi_->val_) > 10)
      return;

    avi_->adj_ += adj_ * grad_reg_lower_inc_gamma(avi_->val_, bd_);
  }
};

class gamma_p_dv_vari : public op_dv_vari {
 public:
  gamma_p_dv_vari(double a, vari* bvi)
      : op_dv_vari(gamma_p(a, bvi->val_), a, bvi) {}
  void chain() {
    if (is_inf(ad_)) {
      bvi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }
    if (is_inf(bvi_->val_)) {
      bvi_->adj_ += std::numeric_limits<double>::quiet_NaN();
      return;
    }

    // return zero derivative as gamma_p is flat to
    // machine precision for b / a > 10
    if (std::fabs(bvi_->val_ / ad_) > 10)
      return;

    bvi_->adj_ += adj_
                  * std::exp(-bvi_->val_ + (ad_ - 1.0) * std::log(bvi_->val_)
                             - lgamma(ad_));
  }
};
}  // namespace internal

inline var gamma_p(const var& a, const var& b) {
  return var(new internal::gamma_p_vv_vari(a.vi_, b.vi_));
}

inline var gamma_p(const var& a, double b) {
  return var(new internal::gamma_p_vd_vari(a.vi_, b));
}

inline var gamma_p(double a, const var& b) {
  return var(new internal::gamma_p_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
