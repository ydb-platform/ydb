#ifndef STAN_MATH_PRIM_SCAL_FUN_PRIMITIVE_VALUE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PRIMITIVE_VALUE_HPP

#include <stan/math/prim/scal/fun/value_of.hpp>
#include <type_traits>

namespace stan {
namespace math {

/**
 * Return the value of the specified arithmetic argument
 * unmodified with its own declared type.
 *
 * <p>This template function can only be instantiated with
 * arithmetic types as defined by std library's
 * <code>is_arithmetic</code> trait metaprogram.
 *
 * <p>This function differs from <code>value_of</code> in that it
 * does not cast all return types to <code>double</code>.
 *
 * @tparam T type of arithmetic input.
 * @param x input.
 * @return input unmodified.
 */
template <typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value, T>::type
primitive_value(T x) {
  return x;
}

/**
 * Return the primitive value of the specified argument.
 *
 * <p>This implementation only applies to non-arithmetic types as
 * defined by std libray's <code>is_arithmetic</code> trait metaprogram.
 *
 * @tparam T type of non-arithmetic input.
 * @param x input.
 * @return value of input.
 */
template <typename T>
inline typename std::enable_if<!std::is_arithmetic<T>::value, double>::type
primitive_value(const T& x) {
  return value_of(x);
}

}  // namespace math
}  // namespace stan
#endif
