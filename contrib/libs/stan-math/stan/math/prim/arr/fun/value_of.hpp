#ifndef STAN_MATH_PRIM_ARR_FUN_VALUE_OF_HPP
#define STAN_MATH_PRIM_ARR_FUN_VALUE_OF_HPP

#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/child_type.hpp>
#include <vector>
#include <cstddef>

namespace stan {
namespace math {

/**
 * Convert a std::vector of type T to a std::vector of
 * child_type<T>::type.
 *
 * @tparam T Scalar type in std::vector
 * @param[in] x std::vector to be converted
 * @return std::vector of values
 **/
template <typename T>
inline std::vector<typename child_type<T>::type> value_of(
    const std::vector<T>& x) {
  size_t size = x.size();
  std::vector<typename child_type<T>::type> result(size);
  for (size_t i = 0; i < size; i++)
    result[i] = value_of(x[i]);
  return result;
}

/**
 * Return the specified argument.
 *
 * <p>See <code>value_of(T)</code> for a polymorphic
 * implementation using static casts.
 *
 * <p>This inline pass-through no-op should be compiled away.
 *
 * @param x Specified std::vector.
 * @return Specified std::vector.
 */
inline const std::vector<double>& value_of(const std::vector<double>& x) {
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
 * @param x Specified std::vector.
 * @return Specified std::vector.
 */
inline const std::vector<int>& value_of(const std::vector<int>& x) { return x; }

}  // namespace math
}  // namespace stan

#endif
