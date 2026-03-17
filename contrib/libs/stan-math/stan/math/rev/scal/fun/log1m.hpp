#ifndef STAN_MATH_REV_SCAL_FUN_LOG1M_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG1M_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>

namespace stan {
namespace math {

namespace internal {
class log1m_vari : public op_v_vari {
 public:
  explicit log1m_vari(vari* avi) : op_v_vari(log1m(avi->val_), avi) {}
  void chain() { avi_->adj_ += adj_ / (avi_->val_ - 1); }
};
}  // namespace internal

/**
 * The log (1 - x) function for variables.
 *
 * The derivative is given by
 *
 * \f$\frac{d}{dx} \log (1 - x) = -\frac{1}{1 - x}\f$.
 *
 * @param a The variable.
 * @return The variable representing log of 1 minus the variable.
 */
inline var log1m(const var& a) { return var(new internal::log1m_vari(a.vi_)); }

}  // namespace math
}  // namespace stan
#endif
