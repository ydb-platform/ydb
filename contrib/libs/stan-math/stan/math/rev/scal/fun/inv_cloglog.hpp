#ifndef STAN_MATH_REV_SCAL_FUN_INV_CLOGLOG_HPP
#define STAN_MATH_REV_SCAL_FUN_INV_CLOGLOG_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/inv_cloglog.hpp>

namespace stan {
namespace math {

namespace internal {
class inv_cloglog_vari : public op_v_vari {
 public:
  explicit inv_cloglog_vari(vari* avi)
      : op_v_vari(inv_cloglog(avi->val_), avi) {}
  void chain() {
    avi_->adj_ += adj_ * std::exp(avi_->val_ - std::exp(avi_->val_));
  }
};
}  // namespace internal

/**
 * Return the inverse complementary log-log function applied
 * specified variable (stan).
 *
 * See inv_cloglog() for the double-based version.
 *
 * The derivative is given by
 *
 * \f$\frac{d}{dx} \mbox{cloglog}^{-1}(x) = \exp (x - \exp (x))\f$.
 *
 * @param a Variable argument.
 * @return The inverse complementary log-log of the specified
 * argument.
 */
inline var inv_cloglog(const var& a) {
  return var(new internal::inv_cloglog_vari(a.vi_));
}

}  // namespace math
}  // namespace stan
#endif
