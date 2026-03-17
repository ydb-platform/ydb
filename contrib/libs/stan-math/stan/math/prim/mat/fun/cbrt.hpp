#ifndef STAN_MATH_PRIM_MAT_FUN_CBRT_HPP
#define STAN_MATH_PRIM_MAT_FUN_CBRT_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/cbrt.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap cbrt() so it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Cube root of x.
 */
struct cbrt_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return cbrt(x);
  }
};

/**
 * Vectorized version of cbrt().
 * @param x Container of variables.
 * @tparam T Container type.
 * @return Cube root of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<cbrt_fun, T>::return_t cbrt(const T& x) {
  return apply_scalar_unary<cbrt_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
