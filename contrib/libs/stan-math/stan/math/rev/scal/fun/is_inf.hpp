#ifndef STAN_MATH_REV_SCAL_FUN_IS_INF_HPP
#define STAN_MATH_REV_SCAL_FUN_IS_INF_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

namespace stan {
namespace math {

/**
 * Returns 1 if the input's value is infinite and 0 otherwise.
 *
 * Delegates to <code>is_inf</code>.
 *
 * @param v Value to test.
 *
 * @return <code>1</code> if the value is infinite and <code>0</code> otherwise.
 */
inline int is_inf(const var& v) { return is_inf(v.val()); }

}  // namespace math
}  // namespace stan
#endif
