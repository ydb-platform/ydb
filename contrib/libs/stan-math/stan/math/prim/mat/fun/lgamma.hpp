#ifndef STAN_MATH_PRIM_MAT_FUN_LGAMMA_HPP
#define STAN_MATH_PRIM_MAT_FUN_LGAMMA_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap lgamma() so that it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Natural log of the gamma function applied to x.
 * @throw std::domain_error if x is a negative integer or 0.
 */
struct lgamma_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return lgamma(x);
  }
};

/**
 * Vectorized version of lgamma().
 * @param x Container.
 * @tparam T Container type.
 * @return Natural log of the gamma function
 *         applied to each value in x.
 * @throw std::domain_error if any value is a negative integer or 0.
 */
template <typename T>
inline typename apply_scalar_unary<lgamma_fun, T>::return_t lgamma(const T& x) {
  return apply_scalar_unary<lgamma_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
