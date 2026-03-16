#ifndef STAN_MATH_FWD_CORE_OPERATOR_UNARY_MINUS_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_UNARY_MINUS_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return the negation of the specified argument.
 *
 * @tparam value and tangent type of the argument
 * @param[in] x argument
 * @return negation of argument
 */
template <typename T>
inline fvar<T> operator-(const fvar<T>& x) {
  return fvar<T>(-x.val_, -x.d_);
}
}  // namespace math
}  // namespace stan
#endif
