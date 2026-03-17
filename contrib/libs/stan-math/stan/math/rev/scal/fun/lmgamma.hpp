#ifndef STAN_MATH_REV_SCAL_FUN_LMGAMMA_HPP
#define STAN_MATH_REV_SCAL_FUN_LMGAMMA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/lmgamma.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class lmgamma_dv_vari : public op_dv_vari {
 public:
  lmgamma_dv_vari(int a, vari* bvi)
      : op_dv_vari(lmgamma(a, bvi->val_), a, bvi) {}
  void chain() {
    double deriv = 0;
    for (int i = 1; i < ad_ + 1; i++)
      deriv += digamma(bvi_->val_ + (1.0 - i) / 2.0);
    bvi_->adj_ += adj_ * deriv;
  }
};
}  // namespace internal

inline var lmgamma(int a, const var& b) {
  return var(new internal::lmgamma_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
