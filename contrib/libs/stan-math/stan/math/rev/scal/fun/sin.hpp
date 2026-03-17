#ifndef STAN_MATH_REV_SCAL_FUN_SIN_HPP
#define STAN_MATH_REV_SCAL_FUN_SIN_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class sin_vari : public op_v_vari {
 public:
  explicit sin_vari(vari* avi) : op_v_vari(std::sin(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * std::cos(avi_->val_); }
};
}  // namespace internal

/**
 * Return the sine of a radian-scaled variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \sin x = \cos x\f$.
 *
 *
   \f[
   \mbox{sin}(x) =
   \begin{cases}
     \sin(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{sin}(x)}{\partial x} =
   \begin{cases}
     \cos(x) & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable for radians of angle.
 * @return Sine of variable.
 */
inline var sin(const var& a) { return var(new internal::sin_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
