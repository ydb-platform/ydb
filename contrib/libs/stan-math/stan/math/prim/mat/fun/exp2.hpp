#ifndef STAN_MATH_PRIM_MAT_FUN_EXP2_HPP
#define STAN_MATH_PRIM_MAT_FUN_EXP2_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/exp2.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap exp2() so it can be vectorized.
 */
struct exp2_fun {
  /**
   * Return the base two exponent of the specified argument.
   *
   * @param x Argument.
   * @return Base two exponent of the argument.
   * @tparam T Argument type.
   */
  template <typename T>
  static inline T fun(const T& x) {
    return exp2(x);
  }
};

/**
 * Return the elementwise application of <code>exp2()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T Container type.
 * @param x Container.
 * @return Elementwise exp2 of members of container.
 */
template <typename T>
inline typename apply_scalar_unary<exp2_fun, T>::return_t exp2(const T& x) {
  return apply_scalar_unary<exp2_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
