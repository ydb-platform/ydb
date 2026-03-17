#ifndef STAN_MATH_PRIM_ARR_FUN_VALUE_OF_REC_HPP
#define STAN_MATH_PRIM_ARR_FUN_VALUE_OF_REC_HPP

#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <vector>
#include <cstddef>

namespace stan {
namespace math {

/**
 * Convert a std::vector of type T to a std::vector of doubles.
 *
 * T must implement value_of_rec. See
 * test/math/fwd/mat/fun/value_of_rec.cpp for fvar and var usage.
 *
 * @tparam T Scalar type in std::vector
 * @param[in] x std::vector to be converted
 * @return std::vector of values
 **/
template <typename T>
inline std::vector<double> value_of_rec(const std::vector<T>& x) {
  size_t size = x.size();
  std::vector<double> result(size);
  for (size_t i = 0; i < size; i++)
    result[i] = value_of_rec(x[i]);
  return result;
}

/**
 * Return the specified argument.
 *
 * <p>See <code>value_of_rec(T)</code> for a polymorphic
 * implementation using static casts.
 *
 * <p>This inline pass-through no-op should be compiled away.
 *
 * @param x Specified std::vector.
 * @return Specified std::vector.
 */
inline const std::vector<double>& value_of_rec(const std::vector<double>& x) {
  return x;
}

}  // namespace math
}  // namespace stan

#endif
