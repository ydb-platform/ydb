#ifndef STAN_MATH_REV_SCAL_FUN_LOG_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>

namespace stan {
namespace math {

namespace internal {
class log_vari : public op_v_vari {
 public:
  explicit log_vari(vari* avi) : op_v_vari(std::log(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ / avi_->val_; }
};
}  // namespace internal

/**
 * Return the natural log of the specified variable (cmath).
 *
 * The derivative is defined by
 *
 * \f$\frac{d}{dx} \log x = \frac{1}{x}\f$.
 *
   \f[
   \mbox{log}(x) =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < 0\\
     \ln(x) & \mbox{if } x \geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log}(x)}{\partial x} =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < 0\\
     \frac{1}{x} & \mbox{if } x\geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a Variable whose log is taken.
 * @return Natural log of variable.
 */
inline var log(const var& a) { return var(new internal::log_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
