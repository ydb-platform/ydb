#ifndef STAN_MATH_REV_SCAL_FUN_EXPM1_HPP
#define STAN_MATH_REV_SCAL_FUN_EXPM1_HPP

#include <stan/math/prim/scal/fun/expm1.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

namespace internal {
class expm1_vari : public op_v_vari {
 public:
  explicit expm1_vari(vari* avi) : op_v_vari(expm1(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * (val_ + 1); }
};
}  // namespace internal

/**
 * The exponentiation of the specified variable minus 1 (C99).
 *
 * The derivative is given by
 *
 * \f$\frac{d}{dx} \exp(a) - 1 = \exp(a)\f$.
 *
 *
   \f[
   \mbox{expm1}(x) =
   \begin{cases}
     e^x-1 & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{expm1}(x)}{\partial x} =
   \begin{cases}
     e^x & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a The variable.
 * @return Two to the power of the specified variable.
 */
inline var expm1(const var& a) { return var(new internal::expm1_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
