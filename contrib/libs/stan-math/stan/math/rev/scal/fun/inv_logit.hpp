#ifndef STAN_MATH_REV_SCAL_FUN_INV_LOGIT_HPP
#define STAN_MATH_REV_SCAL_FUN_INV_LOGIT_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>

namespace stan {
namespace math {

namespace internal {
class inv_logit_vari : public op_v_vari {
 public:
  explicit inv_logit_vari(vari* avi) : op_v_vari(inv_logit(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ * val_ * (1.0 - val_); }
};
}  // namespace internal

/**
 * The inverse logit function for variables (stan).
 *
 * See inv_logit() for the double-based version.
 *
 * The derivative of inverse logit is
 *
 * \f$\frac{d}{dx} \mbox{logit}^{-1}(x) = \mbox{logit}^{-1}(x) (1 -
 * \mbox{logit}^{-1}(x))\f$.
 *
 * @param a Argument variable.
 * @return Inverse logit of argument.
 */
inline var inv_logit(const var& a) {
  return var(new internal::inv_logit_vari(a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
