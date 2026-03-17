#ifndef STAN_MATH_REV_SCAL_FUN_IS_UNINITIALIZED_HPP
#define STAN_MATH_REV_SCAL_FUN_IS_UNINITIALIZED_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_uninitialized.hpp>

namespace stan {
namespace math {

/**
 * Returns <code>true</code> if the specified variable is
 * uninitialized.
 *
 * This overload of the
 * <code>is_uninitialized()</code> function delegates
 * the return to the <code>is_uninitialized()</code> method on the
 * specified variable.
 *
 * @param x Object to test.
 * @return <code>true</code> if the specified object is uninitialized.
 */
inline bool is_uninitialized(var x) { return x.is_uninitialized(); }

}  // namespace math
}  // namespace stan
#endif
