#ifndef STAN_MATH_REV_SCAL_FUN_MODIFIED_BESSEL_FIRST_KIND_HPP
#define STAN_MATH_REV_SCAL_FUN_MODIFIED_BESSEL_FIRST_KIND_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/modified_bessel_first_kind.hpp>

namespace stan {
namespace math {

namespace internal {

class modified_bessel_first_kind_dv_vari : public op_dv_vari {
 public:
  modified_bessel_first_kind_dv_vari(int a, vari* bvi)
      : op_dv_vari(modified_bessel_first_kind(a, bvi->val_), a, bvi) {}
  void chain() {
    bvi_->adj_
        += adj_
           * (-ad_ * modified_bessel_first_kind(ad_, bvi_->val_) / bvi_->val_
              + modified_bessel_first_kind(ad_ - 1, bvi_->val_));
  }
};
}  // namespace internal

inline var modified_bessel_first_kind(int v, const var& a) {
  return var(new internal::modified_bessel_first_kind_dv_vari(v, a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
