#ifndef STAN_MATH_REV_SCAL_FUN_LOGIT_HPP
#define STAN_MATH_REV_SCAL_FUN_LOGIT_HPP

#include <stan/math/prim/scal/fun/logit.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/core/precomp_v_vari.hpp>

namespace stan {
namespace math {

/**
 * Return the log odds of the specified argument.
 *
 * @param u argument
 * @return log odds of argument
 */
inline var logit(const var& u) {
  return var(new precomp_v_vari(logit(u.val()), u.vi_,
                                1 / (u.val() - u.val() * u.val())));
}

}  // namespace math
}  // namespace stan
#endif
