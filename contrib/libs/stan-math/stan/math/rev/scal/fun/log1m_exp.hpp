#ifndef STAN_MATH_REV_SCAL_FUN_LOG1M_EXP_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG1M_EXP_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/expm1.hpp>
#include <stan/math/prim/scal/fun/log1m_exp.hpp>

namespace stan {
namespace math {

namespace internal {
class log1m_exp_v_vari : public op_v_vari {
 public:
  explicit log1m_exp_v_vari(vari* avi) : op_v_vari(log1m_exp(avi->val_), avi) {}

  void chain() { avi_->adj_ -= adj_ / expm1(-(avi_->val_)); }
};
}  // namespace internal

/**
 * Return the log of 1 minus the exponential of the specified
 * variable.
 *
 * <p>The deriative of <code>log(1 - exp(x))</code> with respect
 * to <code>x</code> is <code>-1 / expm1(-x)</code>.
 *
 * @param[in] x Argument.
 * @return Natural logarithm of one minus the exponential of the
 * argument.
 */
inline var log1m_exp(const var& x) {
  return var(new internal::log1m_exp_v_vari(x.vi_));
}

}  // namespace math
}  // namespace stan
#endif
