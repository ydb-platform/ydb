#ifndef STAN_MATH_REV_SCAL_FUN_ATAN_HPP
#define STAN_MATH_REV_SCAL_FUN_ATAN_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class atan_vari : public op_v_vari {
 public:
  explicit atan_vari(vari* avi) : op_v_vari(std::atan(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ / (1.0 + (avi_->val_ * avi_->val_)); }
};
}  // namespace internal

/**
 * Return the principal value of the arc tangent, in radians, of the
 * specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \arctan x = \frac{1}{1 + x^2}\f$.
 *
 *
   \f[
   \mbox{atan}(x) =
   \begin{cases}
     \arctan(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{atan}(x)}{\partial x} =
   \begin{cases}
     \frac{\partial\, \arctan(x)}{\partial x} & \mbox{if } -\infty\leq x\leq
 \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   \frac{\partial \, \arctan(x)}{\partial x} = \frac{1}{x^2+1}
   \f]
 *
 * @param a Variable in range [-1, 1].
 * @return Arc tangent of variable, in radians.
 */
inline var atan(const var& a) { return var(new internal::atan_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
