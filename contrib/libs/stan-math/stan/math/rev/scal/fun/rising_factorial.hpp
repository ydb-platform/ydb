#ifndef STAN_MATH_REV_SCAL_FUN_RISING_FACTORIAL_HPP
#define STAN_MATH_REV_SCAL_FUN_RISING_FACTORIAL_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/rising_factorial.hpp>

namespace stan {
namespace math {

namespace internal {

class rising_factorial_vd_vari : public op_vd_vari {
 public:
  rising_factorial_vd_vari(vari* avi, int b)
      : op_vd_vari(rising_factorial(avi->val_, b), avi, b) {}
  void chain() {
    avi_->adj_ += adj_ * rising_factorial(avi_->val_, bd_)
                  * (digamma(avi_->val_ + bd_) - digamma(avi_->val_));
  }
};
}  // namespace internal

inline var rising_factorial(const var& a, int b) {
  return var(new internal::rising_factorial_vd_vari(a.vi_, b));
}
}  // namespace math
}  // namespace stan
#endif
