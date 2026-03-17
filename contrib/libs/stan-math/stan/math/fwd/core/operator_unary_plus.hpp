#ifndef STAN_MATH_FWD_CORE_OPERATOR_UNARY_PLUS_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_UNARY_PLUS_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Returns the argument.  It is included for completeness.  The
 * primitive unary <code>operator+</code> exists to promote
 * integer to floating point values.
 *
 * @tparam T value and tangent type of the argument
 * @param x argument
 * @return the argument
 */
template <typename T>
inline fvar<T> operator+(const fvar<T>& x) {
  return x;
}

}  // namespace math
}  // namespace stan
#endif
