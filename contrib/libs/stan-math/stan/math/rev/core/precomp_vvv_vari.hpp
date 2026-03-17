#ifndef STAN_MATH_REV_CORE_PRECOMP_VVV_VARI_HPP
#define STAN_MATH_REV_CORE_PRECOMP_VVV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>
#include <stan/math/rev/core/vvv_vari.hpp>

namespace stan {
namespace math {

// use for single precomputed partials
class precomp_vvv_vari : public op_vvv_vari {
 protected:
  double da_;
  double db_;
  double dc_;

 public:
  precomp_vvv_vari(double val, vari* avi, vari* bvi, vari* cvi, double da,
                   double db, double dc)
      : op_vvv_vari(val, avi, bvi, cvi), da_(da), db_(db), dc_(dc) {}
  void chain() {
    avi_->adj_ += adj_ * da_;
    bvi_->adj_ += adj_ * db_;
    cvi_->adj_ += adj_ * dc_;
  }
};

}  // namespace math
}  // namespace stan
#endif
