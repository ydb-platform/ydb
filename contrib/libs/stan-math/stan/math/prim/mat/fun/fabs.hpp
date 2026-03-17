#ifndef STAN_MATH_PRIM_MAT_FUN_FABS_HPP
#define STAN_MATH_PRIM_MAT_FUN_FABS_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap fabs() so that it can be vectorized.
 *
 * @param x Variable.
 * @tparam T Variable type.
 * @return Absolute value of x.
 */
struct fabs_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::fabs;
    return fabs(x);
  }
};

/**
 * Vectorized version of fabs().
 *
 * @param x Container.
 * @tparam T Container type.
 * @return Absolute value of each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<fabs_fun, T>::return_t fabs(const T& x) {
  return apply_scalar_unary<fabs_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
