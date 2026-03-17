#ifndef STAN_MATH_REV_SCAL_FUN_FLOOR_HPP
#define STAN_MATH_REV_SCAL_FUN_FLOOR_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/meta/likely.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class floor_vari : public op_v_vari {
 public:
  explicit floor_vari(vari* avi) : op_v_vari(std::floor(avi->val_), avi) {}
  void chain() {
    if (unlikely(is_nan(avi_->val_)))
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
  }
};
}  // namespace internal

/**
 * Return the floor of the specified variable (cmath).
 *
 * The derivative of the floor function is defined and
 * zero everywhere but at integers, so we set these derivatives
 * to zero for convenience,
 *
 * \f$\frac{d}{dx} {\lfloor x \rfloor} = 0\f$.
 *
 * The floor function rounds down.  For double values, this is the largest
 * integral value that is not greater than the specified value.
 * Although this function is not differentiable because it is
 * discontinuous at integral values, its gradient is returned as
 * zero everywhere.
 *
   \f[
   \mbox{floor}(x) =
   \begin{cases}
     \lfloor x \rfloor & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{floor}(x)}{\partial x} =
   \begin{cases}
     0 & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Input variable.
 * @return Floor of the variable.
 */
inline var floor(const var& a) { return var(new internal::floor_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
