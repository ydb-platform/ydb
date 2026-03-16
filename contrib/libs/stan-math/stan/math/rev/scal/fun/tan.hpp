#ifndef STAN_MATH_REV_SCAL_FUN_TAN_HPP
#define STAN_MATH_REV_SCAL_FUN_TAN_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class tan_vari : public op_v_vari {
 public:
  explicit tan_vari(vari* avi) : op_v_vari(std::tan(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * (1.0 + val_ * val_); }
};
}  // namespace internal

/**
 * Return the tangent of a radian-scaled variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \tan x = \sec^2 x\f$.
 *
 *
   \f[
   \mbox{tan}(x) =
   \begin{cases}
     \tan(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{tan}(x)}{\partial x} =
   \begin{cases}
     \sec^2(x) & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable for radians of angle.
 * @return Tangent of variable.
 */
inline var tan(const var& a) { return var(new internal::tan_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
