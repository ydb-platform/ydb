#ifndef STAN_MATH_REV_SCAL_FUN_AS_BOOL_HPP
#define STAN_MATH_REV_SCAL_FUN_AS_BOOL_HPP

#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

/**
 * Return 1 if the argument is unequal to zero and 0 otherwise.
 *
 * @param v Value.
 * @return 1 if argument is equal to zero (or NaN) and 0 otherwise.
 */
inline int as_bool(const var& v) { return 0.0 != v.vi_->val_; }

}  // namespace math
}  // namespace stan
#endif
