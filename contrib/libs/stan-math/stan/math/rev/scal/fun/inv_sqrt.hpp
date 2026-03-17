#ifndef STAN_MATH_REV_SCAL_FUN_INV_SQRT_HPP
#define STAN_MATH_REV_SCAL_FUN_INV_SQRT_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/inv_sqrt.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class inv_sqrt_vari : public op_v_vari {
 public:
  explicit inv_sqrt_vari(vari* avi) : op_v_vari(inv_sqrt(avi->val_), avi) {}
  void chain() {
    avi_->adj_ -= 0.5 * adj_ / (avi_->val_ * std::sqrt(avi_->val_));
  }
};
}  // namespace internal

/**
 *
   \f[
   \mbox{inv\_sqrt}(x) =
   \begin{cases}
     \frac{1}{\sqrt{x}} & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{inv\_sqrt}(x)}{\partial x} =
   \begin{cases}
     -\frac{1}{2\sqrt{x^3}} & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 */
inline var inv_sqrt(const var& a) {
  return var(new internal::inv_sqrt_vari(a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
