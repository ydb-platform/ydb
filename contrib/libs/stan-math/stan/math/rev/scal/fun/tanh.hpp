#ifndef STAN_MATH_REV_SCAL_FUN_TANH_HPP
#define STAN_MATH_REV_SCAL_FUN_TANH_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class tanh_vari : public op_v_vari {
 public:
  explicit tanh_vari(vari* avi) : op_v_vari(std::tanh(avi->val_), avi) {}
  void chain() {
    double cosh = std::cosh(avi_->val_);
    avi_->adj_ += adj_ / (cosh * cosh);
  }
};
}  // namespace internal

/**
 * Return the hyperbolic tangent of the specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \tanh x = \frac{1}{\cosh^2 x}\f$.
 *
 *
   \f[
   \mbox{tanh}(x) =
   \begin{cases}
     \tanh(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{tanh}(x)}{\partial x} =
   \begin{cases}
     \mbox{sech}^2(x) & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable.
 * @return Hyperbolic tangent of variable.
 */
inline var tanh(const var& a) { return var(new internal::tanh_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
