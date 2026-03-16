#ifndef STAN_MATH_PRIM_MAT_FUN_ERF_HPP
#define STAN_MATH_PRIM_MAT_FUN_ERF_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/erf.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap erf() so it can be vectorized.
 *
 * @tparam T Variable type.
 * @param x Variable.
 * @return Error function of x.
 */
struct erf_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return erf(x);
  }
};

/**
 * Vectorized version of erf().
 *
 * @tparam T Container type.
 * @param x Container.
 * @return Error function applied to each value in x.
 */
template <typename T>
inline typename apply_scalar_unary<erf_fun, T>::return_t erf(const T& x) {
  return apply_scalar_unary<erf_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
