#ifndef STAN_MATH_FWD_SCAL_FUN_IS_INF_HPP
#define STAN_MATH_FWD_SCAL_FUN_IS_INF_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>

namespace stan {
namespace math {

/**
 * Returns 1 if the input's value is infinite and 0 otherwise.
 *
 * Delegates to <code>is_inf</code>.
 *
 * @param x Value to test.
 * @return <code>1</code> if the value is infinite and <code>0</code> otherwise.
 */
template <typename T>
inline int is_inf(const fvar<T>& x) {
  return is_inf(x.val());
}

}  // namespace math
}  // namespace stan
#endif
