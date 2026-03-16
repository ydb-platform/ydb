#ifndef STAN_MATH_PRIM_MAT_FUN_INV_PHI_HPP
#define STAN_MATH_PRIM_MAT_FUN_INV_PHI_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/inv_Phi.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap inv_Phi() so it can be vectorized.
 * @param x Variable in range [0, 1].
 * @tparam T Variable type.
 * @return Inverse unit normal CDF of x.
 * @throw std::domain_error if x is not between 0 and 1.
 */
struct inv_Phi_fun {
  template <typename T>
  static inline T fun(const T& x) {
    return inv_Phi(x);
  }
};

/**
 * Vectorized version of inv_Phi().
 * @param x Container of variables in range [0, 1].
 * @tparam T Container type.
 * @return Inverse unit normal CDF of each value in x.
 * @throw std::domain_error if any value is not between 0 and 1.
 */
template <typename T>
inline typename apply_scalar_unary<inv_Phi_fun, T>::return_t inv_Phi(
    const T& x) {
  return apply_scalar_unary<inv_Phi_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
