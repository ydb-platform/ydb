#ifndef STAN_MATH_PRIM_SCAL_FUN_VALUE_OF_HPP
#define STAN_MATH_PRIM_SCAL_FUN_VALUE_OF_HPP

namespace stan {
namespace math {

/**
 * Return the value of the specified scalar argument
 * converted to a double value.
 *
 * <p>See the <code>primitive_value</code> function to
 * extract values without casting to <code>double</code>.
 *
 * <p>This function is meant to cover the primitive types. For
 * types requiring pass-by-reference, this template function
 * should be specialized.
 *
 * @tparam T type of scalar.
 * @param x scalar to convert to double
 * @return value of scalar cast to double
 */
template <typename T>
inline double value_of(const T x) {
  return static_cast<double>(x);
}

/**
 * Return the specified argument.
 *
 * <p>See <code>value_of(T)</code> for a polymorphic
 * implementation using static casts.
 *
 * <p>This inline pass-through no-op should be compiled away.
 *
 * @param x value
 * @return input value
 */
template <>
inline double value_of<double>(double x) {
  return x;
}

/**
 * Return the specified argument.
 *
 * <p>See <code>value_of(T)</code> for a polymorphic
 * implementation using static casts.
 *
 * <p>This inline pass-through no-op should be compiled away.
 *
 * @param x value
 * @return input value
 */
inline int value_of(int x) { return x; }

}  // namespace math
}  // namespace stan
#endif
