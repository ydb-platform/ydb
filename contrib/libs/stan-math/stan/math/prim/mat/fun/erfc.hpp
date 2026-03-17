#ifndef STAN_MATH_PRIM_MAT_FUN_ERFC_HPP
#define STAN_MATH_PRIM_MAT_FUN_ERFC_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/erfc.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap erfc() so that it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Complementary error function applied to x.
 */
struct erfc_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return erfc(x);
  }
};

/**
 * Vectorized version of erfc().
 * @param x Container.
 * @tparam T Container type.
 * @return Complementary error function applied to each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<erfc_fun, T>::return_t erfc(const T& x) {
  return apply_scalar_unary<erfc_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
