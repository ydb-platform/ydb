#ifndef STAN_MATH_REV_SCAL_FUN_EXP2_HPP
#define STAN_MATH_REV_SCAL_FUN_EXP2_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <cmath>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class exp2_vari : public op_v_vari {
 public:
  explicit exp2_vari(vari* avi) : op_v_vari(std::pow(2.0, avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * val_ * LOG_2; }
};
}  // namespace internal

/**
 * Exponentiation base 2 function for variables (C99).
 *
 * The derivative is
 *
 * \f$\frac{d}{dx} 2^x = (\log 2) 2^x\f$.
 *
   \f[
   \mbox{exp2}(x) =
   \begin{cases}
     2^x & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{exp2}(x)}{\partial x} =
   \begin{cases}
     2^x\ln2 & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a The variable.
 * @return Two to the power of the specified variable.
 */
inline var exp2(const var& a) { return var(new internal::exp2_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
