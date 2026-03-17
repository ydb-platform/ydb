#ifndef STAN_MATH_PRIM_MAT_FUN_INV_SQUARE_HPP
#define STAN_MATH_PRIM_MAT_FUN_INV_SQUARE_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/inv_square.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap inv_square() so that it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return 1 / x squared.
 */
struct inv_square_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return inv_square(x);
  }
};

/**
 * Vectorized version of inv_square().
 * @param x Container.
 * @tparam T Container type.
 * @return 1 / the square of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<inv_square_fun, T>::return_t inv_square(
    const T& x) {
  return apply_scalar_unary<inv_square_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
