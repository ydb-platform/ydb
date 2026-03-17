#ifndef STAN_MATH_FWD_CORE_OPERATOR_UNARY_NOT_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_UNARY_NOT_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return the negation of the value of the argument as defined by
 * <code>!</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x argument
 * @return negation of argument value
 */
template <typename T>
inline bool operator!(const fvar<T>& x) {
  return !x.val_;
}

}  // namespace math
}  // namespace stan
#endif
