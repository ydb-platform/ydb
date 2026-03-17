#ifndef STAN_MATH_REV_SCAL_FUN_ERF_HPP
#define STAN_MATH_REV_SCAL_FUN_ERF_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/erf.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class erf_vari : public op_v_vari {
 public:
  explicit erf_vari(vari* avi) : op_v_vari(erf(avi->val_), avi) {}
  void chain() {
    avi_->adj_ += adj_ * TWO_OVER_SQRT_PI * std::exp(-avi_->val_ * avi_->val_);
  }
};
}  // namespace internal

/**
 * The error function for variables (C99).
 *
 * The derivative is
 *
 * \f$\frac{d}{dx} \mbox{erf}(x) = \frac{2}{\sqrt{\pi}} \exp(-x^2)\f$.
 *
 *
   \f[
   \mbox{erf}(x) =
   \begin{cases}
     \operatorname{erf}(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{erf}(x)}{\partial x} =
   \begin{cases}
     \frac{\partial\, \operatorname{erf}(x)}{\partial x} & \mbox{if }
 -\infty\leq x\leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \operatorname{erf}(x)=\frac{2}{\sqrt{\pi}}\int_0^x e^{-t^2}dt
   \f]

   \f[
   \frac{\partial \, \operatorname{erf}(x)}{\partial x} = \frac{2}{\sqrt{\pi}}
 e^{-x^2} \f]
 *
 * @param a The variable.
 * @return Error function applied to the variable.
 */
inline var erf(const var& a) { return var(new internal::erf_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
