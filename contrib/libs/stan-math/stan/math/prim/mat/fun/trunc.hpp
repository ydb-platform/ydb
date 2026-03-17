#ifndef STAN_MATH_PRIM_MAT_FUN_TRUNC_HPP
#define STAN_MATH_PRIM_MAT_FUN_TRUNC_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/trunc.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap trunc() so it can be vectorized.
 */
struct trunc_fun {
  /**
   * Return the truncation of the specified argument to the
   * nearest value.
   *
   * @tparam T argument type
   * @param x argument
   * @return truncation of the argument
   */
  template <typename T>
  static inline T fun(const T& x) {
    return trunc(x);
  }
};

/**
 * Return the elementwise application of <code>trunc()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise trunc of container elements
 */
template <typename T>
inline typename apply_scalar_unary<trunc_fun, T>::return_t trunc(const T& x) {
  return apply_scalar_unary<trunc_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
