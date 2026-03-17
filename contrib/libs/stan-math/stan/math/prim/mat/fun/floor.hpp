#ifndef STAN_MATH_PRIM_MAT_FUN_FLOOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_FLOOR_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap floor() so that it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Greatest integer <= x.
 */
struct floor_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::floor;
    return floor(x);
  }
};

/**
 * Vectorized version of floor().
 * @param x Container.
 * @tparam T Container type.
 * @return Greatest integer <= each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<floor_fun, T>::return_t floor(const T& x) {
  return apply_scalar_unary<floor_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
