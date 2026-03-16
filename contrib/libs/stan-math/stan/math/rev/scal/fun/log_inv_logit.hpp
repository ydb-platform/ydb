#ifndef STAN_MATH_REV_SCAL_FUN_LOG_INV_LOGIT_HPP
#define STAN_MATH_REV_SCAL_FUN_LOG_INV_LOGIT_HPP

#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/core/precomp_v_vari.hpp>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of the inverse logit of the
 * specified argument.
 *
 * @param u argument
 * @return log inverse logit of the argument
 */
inline var log_inv_logit(const var& u) {
  return var(
      new precomp_v_vari(log_inv_logit(u.val()), u.vi_, inv_logit(-u.val())));
}

}  // namespace math
}  // namespace stan
#endif
