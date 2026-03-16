#ifndef STAN_MATH_REV_SCAL_FUN_IS_NAN_HPP
#define STAN_MATH_REV_SCAL_FUN_IS_NAN_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

namespace stan {
namespace math {

/**
 * Returns 1 if the input's value is NaN and 0 otherwise.
 *
 * Delegates to <code>is_nan(double)</code>.
 *
 * @param v Value to test.
 *
 * @return <code>1</code> if the value is NaN and <code>0</code> otherwise.
 */
inline bool is_nan(const var& v) { return is_nan(v.val()); }

}  // namespace math
}  // namespace stan
#endif
