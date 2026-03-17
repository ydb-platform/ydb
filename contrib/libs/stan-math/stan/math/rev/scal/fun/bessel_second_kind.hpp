#ifndef STAN_MATH_REV_SCAL_FUN_BESSEL_SECOND_KIND_HPP
#define STAN_MATH_REV_SCAL_FUN_BESSEL_SECOND_KIND_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/bessel_second_kind.hpp>

namespace stan {
namespace math {

namespace internal {

class bessel_second_kind_dv_vari : public op_dv_vari {
 public:
  bessel_second_kind_dv_vari(int a, vari* bvi)
      : op_dv_vari(bessel_second_kind(a, bvi->val_), a, bvi) {}
  void chain() {
    bvi_->adj_ += adj_
                  * (ad_ * bessel_second_kind(ad_, bvi_->val_) / bvi_->val_
                     - bessel_second_kind(ad_ + 1, bvi_->val_));
  }
};
}  // namespace internal

inline var bessel_second_kind(int v, const var& a) {
  return var(new internal::bessel_second_kind_dv_vari(v, a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
