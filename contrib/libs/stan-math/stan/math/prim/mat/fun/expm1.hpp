#ifndef STAN_MATH_PRIM_MAT_FUN_EXPM1_HPP
#define STAN_MATH_PRIM_MAT_FUN_EXPM1_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/expm1.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap expm1() so that it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Natural exponential of x minus one.
 */
struct expm1_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return expm1(x);
  }
};

/**
 * Vectorized version of expm1().
 * @param x Container.
 * @tparam T Container type.
 * @return Natural exponential of each value in x minus one.
 */
template <typename T>
inline typename apply_scalar_unary<expm1_fun, T>::return_t expm1(const T& x) {
  return apply_scalar_unary<expm1_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
