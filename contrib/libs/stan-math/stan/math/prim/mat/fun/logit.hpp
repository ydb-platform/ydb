#ifndef STAN_MATH_PRIM_MAT_FUN_LOGIT_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOGIT_HPP

#include <stan/math/prim/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/scal/fun/logit.hpp>

namespace stan {
namespace math {

/**
 * Structure to wrap logit() so it can be vectorized.
 */
struct logit_fun {
  /**
   * Return the log odds of the specified argument.
   *
   * @tparam T argument type
   * @param x argument
   * @return log odds of the argument
   */
  template <typename T>
  static inline T fun(const T& x) {
    return logit(x);
  }
};

/**
 * Return the elementwise application of <code>logit()</code> to
 * specified argument container.  The return type promotes the
 * underlying scalar argument type to double if it is an integer,
 * and otherwise is the argument type.
 *
 * @tparam T container type
 * @param x container
 * @return elementwise logit of container elements
 */
template <typename T>
inline typename apply_scalar_unary<logit_fun, T>::return_t logit(const T& x) {
  return apply_scalar_unary<logit_fun, T>::apply(x);
}

}  // namespace math
}  // namespace stan

#endif
