#ifndef STAN_MATH_PRIM_MAT_FUN_LOG10_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG10_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap log10() so it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Log base-10 of x.
 */
struct log10_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using std::log10;
    return log10(x);
  }
};

/**
 * Vectorized version of log10().
 * @param x Container.
 * @tparam T Container type.
 * @return Log base-10 applied to each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<log10_fun, T>::return_t log10(const T& x) {
  return apply_scalar_unary<log10_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
