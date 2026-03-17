#ifndef STAN_MATH_PRIM_MAT_FUN_DIGAMMA_HPP
#define STAN_MATH_PRIM_MAT_FUN_DIGAMMA_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <boost/math/special_functions/digamma.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap digamma() so it can be vectorized.
 * @param x Variable.
 * @tparam T Variable type.
 * @return Digamma function applied to x.
 * @throw std::domain_error if x is a negative integer or 0
 */
struct digamma_fun {
  template <typename T>
  static inline T fun(const T& x) {
    using boost::math::digamma;
    return digamma(x);
  }
};

/**
 * Vectorized version of digamma().
 * @param x Container.
 * @tparam T Container type.
 * @return Digamma function applied to each value in x.
 * @throw std::domain_error if any value is a negative integer or 0
 */
template <typename T>
inline typename apply_scalar_unary<digamma_fun, T>::return_t digamma(
    const T& x) {
  return apply_scalar_unary<digamma_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
