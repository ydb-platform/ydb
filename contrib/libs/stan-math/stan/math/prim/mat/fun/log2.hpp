#ifndef STAN_MATH_PRIM_MAT_FUN_LOG2_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG2_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/log2.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap log2() so it can be vectorized.
 */
struct log2_fun {
  /**
   * Return the base two logarithm of the specified argument.
   *
   * @tparam T argument type
   * @param x argument
   * @return base two log of the argument
   */
  template <typename T>
  static inline T fun(const T& x) {
    return log2(x);
  }
};

/**
 * Return the elementwise application of <code>log2()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise log2 of container elements
 */
template <typename T>
inline typename apply_scalar_unary<log2_fun, T>::return_t log2(const T& x) {
  return apply_scalar_unary<log2_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
