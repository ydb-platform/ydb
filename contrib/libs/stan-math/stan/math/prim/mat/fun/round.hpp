#ifndef STAN_MATH_PRIM_MAT_FUN_ROUND_HPP
#define STAN_MATH_PRIM_MAT_FUN_ROUND_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/round.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap round() so it can be vectorized.
 * @param x Argument variable.
 * @tparam T Argument type.
 * @return Rounded value of x.
 */
struct round_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using stan::math::round;
    return round(x);
  }
};

/**
 * Vectorized version of round.
 * @param x Container.
 * @tparam T Container type.
 * @return Rounded value of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<round_fun, T>::return_t round(const T& x) {
  return apply_scalar_unary<round_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
