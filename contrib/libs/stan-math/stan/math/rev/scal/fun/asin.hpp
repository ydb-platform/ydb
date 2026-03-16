#ifndef STAN_MATH_REV_SCAL_FUN_ASIN_HPP
#define STAN_MATH_REV_SCAL_FUN_ASIN_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class asin_vari : public op_v_vari {
 public:
  explicit asin_vari(vari* avi) : op_v_vari(std::asin(avi->val_), avi) {}
  void chain() {
    avi_->adj_ += adj_ / std::sqrt(1.0 - (avi_->val_ * avi_->val_));
  }
};
}  // namespace internal

/**
 * Return the principal value of the arc sine, in radians, of the
 * specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \arcsin x = \frac{1}{\sqrt{1 - x^2}}\f$.
 *
 *
   \f[
   \mbox{asin}(x) =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < -1\\
     \arcsin(x) & \mbox{if } -1\leq x\leq 1 \\
     \textrm{NaN} & \mbox{if } x > 1\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{asin}(x)}{\partial x} =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < -1\\
     \frac{\partial\, \arcsin(x)}{\partial x} & \mbox{if } -1\leq x\leq 1 \\
     \textrm{NaN} & \mbox{if } x < -1\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial \, \arcsin(x)}{\partial x} = \frac{1}{\sqrt{1-x^2}}
   \f]
 *
 * @param a Variable in range [-1, 1].
 * @return Arc sine of variable, in radians.
 */
inline var asin(const var& a) { return var(new internal::asin_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
