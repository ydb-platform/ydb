#ifndef STAN_MATH_FWD_SCAL_FUN_PRIMITIVE_VALUE_HPP
#define STAN_MATH_FWD_SCAL_FUN_PRIMITIVE_VALUE_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/primitive_value.hpp>

namespace stan {
namespace math {

/**
 * Return the primitive value of the specified forward-mode
 * autodiff variable.  This function applies recursively to
 * higher-order autodiff types to return a primitive double value.
 *
 * @tparam T scalar type for autodiff variable.
 * @param v input variable.
 * @return primitive value of input.
 */
template <typename T>
inline double primitive_value(const fvar<T>& v) {
  return primitive_value(v.val_);
}

}  // namespace math

}  // namespace stan

#endif
