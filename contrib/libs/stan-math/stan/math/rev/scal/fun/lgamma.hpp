#ifndef STAN_MATH_REV_SCAL_FUN_LGAMMA_HPP
#define STAN_MATH_REV_SCAL_FUN_LGAMMA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>

namespace stan {
namespace math {

namespace internal {
class lgamma_vari : public op_v_vari {
 public:
  lgamma_vari(double value, vari* avi) : op_v_vari(value, avi) {}
  void chain() { avi_->adj_ += adj_ * digamma(avi_->val_); }
};
}  // namespace internal

/**
 * The log gamma function for variables (C99).
 *
 * The derivatie is the digamma function,
 *
 * \f$\frac{d}{dx} \Gamma(x) = \psi^{(0)}(x)\f$.
 *
 * @param a The variable.
 * @return Log gamma of the variable.
 */
inline var lgamma(const var& a) {
  return var(new internal::lgamma_vari(lgamma(a.val()), a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
