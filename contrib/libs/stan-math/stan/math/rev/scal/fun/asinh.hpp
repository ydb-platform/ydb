#ifndef STAN_MATH_REV_SCAL_FUN_ASINH_HPP
#define STAN_MATH_REV_SCAL_FUN_ASINH_HPP

#include <stan/math/prim/scal/fun/asinh.hpp>
#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class asinh_vari : public op_v_vari {
 public:
  asinh_vari(double val, vari* avi) : op_v_vari(val, avi) {}
  void chain() {
    avi_->adj_ += adj_ / std::sqrt(avi_->val_ * avi_->val_ + 1.0);
  }
};
}  // namespace internal

/**
 * The inverse hyperbolic sine function for variables (C99).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \mbox{asinh}(x) = \frac{x}{x^2 + 1}\f$.
 *
 *
   \f[
   \mbox{asinh}(x) =
   \begin{cases}
     \sinh^{-1}(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{asinh}(x)}{\partial x} =
   \begin{cases}
     \frac{\partial\, \sinh^{-1}(x)}{\partial x} & \mbox{if } -\infty\leq x\leq
 \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   \sinh^{-1}(x)=\ln\left(x+\sqrt{x^2+1}\right)
   \f]

   \f[
   \frac{\partial \, \sinh^{-1}(x)}{\partial x} = \frac{1}{\sqrt{x^2+1}}
   \f]
 *
 * @param a The variable.
 * @return Inverse hyperbolic sine of the variable.
 */
inline var asinh(const var& a) {
  return var(new internal::asinh_vari(asinh(a.val()), a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
