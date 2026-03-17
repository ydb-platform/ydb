#ifndef STAN_MATH_REV_SCAL_FUN_COS_HPP
#define STAN_MATH_REV_SCAL_FUN_COS_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class cos_vari : public op_v_vari {
 public:
  explicit cos_vari(vari* avi) : op_v_vari(std::cos(avi->val_), avi) {}
  void chain() { avi_->adj_ -= adj_ * std::sin(avi_->val_); }
};
}  // namespace internal

/**
 * Return the cosine of a radian-scaled variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \cos x = - \sin x\f$.
 *
 *
   \f[
   \mbox{cos}(x) =
   \begin{cases}
     \cos(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{cos}(x)}{\partial x} =
   \begin{cases}
     -\sin(x) & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable for radians of angle.
 * @return Cosine of variable.
 */
inline var cos(const var& a) { return var(new internal::cos_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
