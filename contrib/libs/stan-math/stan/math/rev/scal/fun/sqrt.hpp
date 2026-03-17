#ifndef STAN_MATH_REV_SCAL_FUN_SQRT_HPP
#define STAN_MATH_REV_SCAL_FUN_SQRT_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class sqrt_vari : public op_v_vari {
 public:
  explicit sqrt_vari(vari* avi) : op_v_vari(std::sqrt(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ / (2.0 * val_); }
};
}  // namespace internal

/**
 * Return the square root of the specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \sqrt{x} = \frac{1}{2 \sqrt{x}}\f$.
 *
   \f[
   \mbox{sqrt}(x) =
   \begin{cases}
     \textrm{NaN} & x < 0 \\
     \sqrt{x} & \mbox{if } x\geq 0\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{sqrt}(x)}{\partial x} =
   \begin{cases}
     \textrm{NaN} & x < 0 \\
     \frac{1}{2\sqrt{x}} & x\geq 0\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable whose square root is taken.
 * @return Square root of variable.
 */
inline var sqrt(const var& a) { return var(new internal::sqrt_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
