#ifndef STAN_MATH_REV_SCAL_FUN_PHI_HPP
#define STAN_MATH_REV_SCAL_FUN_PHI_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/Phi.hpp>

namespace stan {
namespace math {

namespace internal {
class Phi_vari : public op_v_vari {
 public:
  explicit Phi_vari(vari* avi) : op_v_vari(Phi(avi->val_), avi) {}
  void chain() {
    static const double NEG_HALF = -0.5;
    avi_->adj_ += adj_ * INV_SQRT_TWO_PI
                  * std::exp(NEG_HALF * avi_->val_ * avi_->val_);
  }
};
}  // namespace internal

/**
 * The unit normal cumulative density function for variables (stan).
 *
 * See Phi() for the double-based version.
 *
 * The derivative is the unit normal density function,
 *
 * \f$\frac{d}{dx} \Phi(x) = \mbox{\sf Norm}(x|0, 1) = \frac{1}{\sqrt{2\pi}}
 \exp(-\frac{1}{2} x^2)\f$.
 *
 *
   \f[
   \mbox{Phi}(x) =
   \begin{cases}
     0 & \mbox{if } x < -37.5 \\
     \Phi(x) & \mbox{if } -37.5 \leq x \leq 8.25 \\
     1 & \mbox{if } x > 8.25 \\[6pt]
     \textrm{error} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{Phi}(x)}{\partial x} =
   \begin{cases}
     0 & \mbox{if } x < -27.5 \\
     \frac{\partial\, \Phi(x)}{\partial x} & \mbox{if } -27.5 \leq x \leq 27.5
 \\ 0 & \mbox{if } x > 27.5 \\[6pt] \textrm{error} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \Phi(x) = \frac{1}{\sqrt{2\pi}} \int_{0}^{x} e^{-t^2/2} dt
   \f]

   \f[
   \frac{\partial \, \Phi(x)}{\partial x} = \frac{e^{-x^2/2}}{\sqrt{2\pi}}
   \f]
 *
 * @param a Variable argument.
 * @return The unit normal cdf evaluated at the specified argument.
 */
inline var Phi(const var& a) { return var(new internal::Phi_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
