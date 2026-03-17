#ifndef STAN_MATH_PRIM_MAT_FUN_LOG1M_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG1M_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap log1m() so it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Natural log of (1 - x).
 */
struct log1m_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return log1m(x);
  }
};

/**
 * Vectorized version of log1m().
 * @param x Container.
 * @tparam T Container type.
 * @return Natural log of 1 minus each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<log1m_fun, T>::return_t log1m(const T& x) {
  return apply_scalar_unary<log1m_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
