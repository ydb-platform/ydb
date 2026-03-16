#ifndef STAN_MATH_REV_SCAL_FUN_INC_BETA_HPP
#define STAN_MATH_REV_SCAL_FUN_INC_BETA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/rev/scal/fun/pow.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {

class inc_beta_vvv_vari : public op_vvv_vari {
 public:
  inc_beta_vvv_vari(vari* avi, vari* bvi, vari* cvi)
      : op_vvv_vari(inc_beta(avi->val_, bvi->val_, cvi->val_), avi, bvi, cvi) {}
  void chain() {
    double d_a;
    double d_b;
    grad_reg_inc_beta(d_a, d_b, avi_->val_, bvi_->val_, cvi_->val_,
                      digamma(avi_->val_), digamma(bvi_->val_),
                      digamma(avi_->val_ + bvi_->val_),
                      std::exp(lbeta(avi_->val_, bvi_->val_)));

    avi_->adj_ += adj_ * d_a;
    bvi_->adj_ += adj_ * d_b;
    cvi_->adj_ += adj_ * std::pow(1 - cvi_->val_, bvi_->val_ - 1)
                  * std::pow(cvi_->val_, avi_->val_ - 1)
                  / std::exp(lbeta(avi_->val_, bvi_->val_));
  }
};

}  // namespace internal

inline var inc_beta(const var& a, const var& b, const var& c) {
  return var(new internal::inc_beta_vvv_vari(a.vi_, b.vi_, c.vi_));
}

}  // namespace math
}  // namespace stan
#endif
