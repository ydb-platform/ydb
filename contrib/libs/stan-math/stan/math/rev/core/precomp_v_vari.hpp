#ifndef STAN_MATH_REV_CORE_PRECOMP_V_VARI_HPP
#define STAN_MATH_REV_CORE_PRECOMP_V_VARI_HPP

#include <stan/math/rev/core/vari.hpp>
#include <stan/math/rev/core/v_vari.hpp>

namespace stan {
namespace math {

// use for single precomputed partials
class precomp_v_vari : public op_v_vari {
 protected:
  double da_;

 public:
  precomp_v_vari(double val, vari* avi, double da)
      : op_v_vari(val, avi), da_(da) {}
  void chain() { avi_->adj_ += adj_ * da_; }
};

}  // namespace math
}  // namespace stan
#endif
