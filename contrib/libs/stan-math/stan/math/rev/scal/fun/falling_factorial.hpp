#ifndef STAN_MATH_REV_SCAL_FUN_FALLING_FACTORIAL_HPP
#define STAN_MATH_REV_SCAL_FUN_FALLING_FACTORIAL_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/falling_factorial.hpp>

namespace stan {
namespace math {

namespace internal {

class falling_factorial_vd_vari : public op_vd_vari {
 public:
  falling_factorial_vd_vari(vari* avi, int b)
      : op_vd_vari(falling_factorial(avi->val_, b), avi, b) {}
  void chain() {
    avi_->adj_ += adj_ * val_
                  * (digamma(avi_->val_ + 1) - digamma(avi_->val_ - bd_ + 1));
  }
};
}  // namespace internal

inline var falling_factorial(const var& a, int b) {
  return var(new internal::falling_factorial_vd_vari(a.vi_, b));
}

}  // namespace math
}  // namespace stan
#endif
