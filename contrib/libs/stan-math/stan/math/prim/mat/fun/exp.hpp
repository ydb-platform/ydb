#ifndef STAN_MATH_PRIM_MAT_FUN_EXP_HPP
#define STAN_MATH_PRIM_MAT_FUN_EXP_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/exp.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Structure to wrap <code>exp()</code> so that it can be
 * vectorized.
 */
struct exp_fun {
  /**
   * Return the exponential of the specified scalar argument.
   *
   * @tparam T Scalar argument type.
   * @param[in] x Argument.
   * @return Exponential of argument.
   */
  template <typename T>
  static inline T fun(const T& x) {
    using std::exp;
    return exp(x);
  }
};

/**
 * Return the elementwise exponentiation of the specified argument,
 * which may be a scalar or any Stan container of numeric scalars.
 * The return type is the same as the argument type.
 *
 * @tparam T Argument type.
 * @param[in] x Argument.
 * @return Elementwise application of exponentiation to the argument.
 */
template <typename T>
inline typename apply_scalar_unary<exp_fun, T>::return_t exp(const T& x) {
  return apply_scalar_unary<exp_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
