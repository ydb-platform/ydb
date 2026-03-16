#ifndef STAN_MATH_REV_SCAL_FUN_INV_SQUARE_HPP
#define STAN_MATH_REV_SCAL_FUN_INV_SQUARE_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/inv_square.hpp>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class inv_square_vari : public op_v_vari {
 public:
  explicit inv_square_vari(vari* avi) : op_v_vari(inv_square(avi->val_), avi) {}
  void chain() {
    avi_->adj_ -= 2 * adj_ / (avi_->val_ * avi_->val_ * avi_->val_);
  }
};
}  // namespace internal

/**
 *
   \f[
   \mbox{inv\_square}(x) =
   \begin{cases}
     \frac{1}{x^2} & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{inv\_square}(x)}{\partial x} =
   \begin{cases}
     -\frac{2}{x^3} & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 */
inline var inv_square(const var& a) {
  return var(new internal::inv_square_vari(a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
