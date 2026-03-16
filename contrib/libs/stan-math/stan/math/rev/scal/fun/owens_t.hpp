#ifndef STAN_MATH_REV_SCAL_FUN_OWENS_T_HPP
#define STAN_MATH_REV_SCAL_FUN_OWENS_T_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/erf.hpp>
#include <stan/math/prim/scal/fun/owens_t.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class owens_t_vv_vari : public op_vv_vari {
 public:
  owens_t_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(owens_t(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    const double neg_avi_sq_div_2 = -square(avi_->val_) * 0.5;
    const double one_p_bvi_sq = 1.0 + square(bvi_->val_);

    avi_->adj_ += adj_ * erf(bvi_->val_ * avi_->val_ * INV_SQRT_2)
                  * std::exp(neg_avi_sq_div_2) * INV_SQRT_TWO_PI * -0.5;
    bvi_->adj_ += adj_ * std::exp(neg_avi_sq_div_2 * one_p_bvi_sq)
                  / (one_p_bvi_sq * 2.0 * pi());
  }
};

class owens_t_vd_vari : public op_vd_vari {
 public:
  owens_t_vd_vari(vari* avi, double b)
      : op_vd_vari(owens_t(avi->val_, b), avi, b) {}
  void chain() {
    avi_->adj_ += adj_ * erf(bd_ * avi_->val_ * INV_SQRT_2)
                  * std::exp(-square(avi_->val_) * 0.5) * INV_SQRT_TWO_PI
                  * -0.5;
  }
};

class owens_t_dv_vari : public op_dv_vari {
 public:
  owens_t_dv_vari(double a, vari* bvi)
      : op_dv_vari(owens_t(a, bvi->val_), a, bvi) {}
  void chain() {
    const double one_p_bvi_sq = 1.0 + square(bvi_->val_);
    bvi_->adj_ += adj_ * std::exp(-0.5 * square(ad_) * one_p_bvi_sq)
                  / (one_p_bvi_sq * 2.0 * pi());
  }
};
}  // namespace internal

/**
 * The Owen's T function of h and a.
 *
 * Used to compute the cumulative density function for the skew normal
 * distribution.
 *
 * @param h var parameter.
 * @param a var parameter.
 * @return The Owen's T function.
 */
inline var owens_t(const var& h, const var& a) {
  return var(new internal::owens_t_vv_vari(h.vi_, a.vi_));
}

/**
 * The Owen's T function of h and a.
 *
 * Used to compute the cumulative density function for the skew normal
 * distribution.
 *
 * @param h var parameter.
 * @param a double parameter.
 * @return The Owen's T function.
 */
inline var owens_t(const var& h, double a) {
  return var(new internal::owens_t_vd_vari(h.vi_, a));
}

/**
 * The Owen's T function of h and a.
 *
 * Used to compute the cumulative density function for the skew normal
 * distribution.
 *
 * @param h double parameter.
 * @param a var parameter.
 * @return The Owen's T function.
 */
inline var owens_t(double h, const var& a) {
  return var(new internal::owens_t_dv_vari(h, a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
