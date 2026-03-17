#ifndef STAN_MATH_REV_SCAL_FUN_SINH_HPP
#define STAN_MATH_REV_SCAL_FUN_SINH_HPP

#include <stan/math/rev/core.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class sinh_vari : public op_v_vari {
 public:
  explicit sinh_vari(vari* avi) : op_v_vari(std::sinh(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * std::cosh(avi_->val_); }
};
}  // namespace internal

/**
 * Return the hyperbolic sine of the specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \sinh x = \cosh x\f$.
 *
 *
   \f[
   \mbox{sinh}(x) =
   \begin{cases}
     \sinh(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{sinh}(x)}{\partial x} =
   \begin{cases}
     \cosh(x) & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable.
 * @return Hyperbolic sine of variable.
 */
inline var sinh(const var& a) { return var(new internal::sinh_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
