#ifndef STAN_MATH_REV_SCAL_FUN_TGAMMA_HPP
#define STAN_MATH_REV_SCAL_FUN_TGAMMA_HPP

#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/tgamma.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

namespace internal {
class tgamma_vari : public op_v_vari {
 public:
  explicit tgamma_vari(vari* avi) : op_v_vari(tgamma(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * val_ * digamma(avi_->val_); }
};
}  // namespace internal

/**
 * Return the Gamma function applied to the specified variable (C99).
 *
 * The derivative with respect to the argument is
 *
 * \f$\frac{d}{dx} \Gamma(x) = \Gamma(x) \Psi^{(0)}(x)\f$
 *
 * where \f$\Psi^{(0)}(x)\f$ is the digamma function.
 *
   \f[
   \mbox{tgamma}(x) =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \Gamma(x) & \mbox{if } x\not\in \{\dots, -3, -2, -1, 0\}\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{tgamma}(x)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \frac{\partial\, \Gamma(x)}{\partial x} & \mbox{if } x\not\in \{\dots, -3,
 -2, -1, 0\}\\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   \Gamma(x)=\int_0^{\infty} u^{x - 1} \exp(-u) \, du
   \f]

   \f[
   \frac{\partial \, \Gamma(x)}{\partial x} = \Gamma(x)\Psi(x)
   \f]
 *
 * @param a Argument to function.
 * @return The Gamma function applied to the specified argument.
 */
inline var tgamma(const var& a) {
  return var(new internal::tgamma_vari(a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
