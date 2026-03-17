#ifndef STAN_MATH_PRIM_SCAL_FUN_FMA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_FMA_HPP

#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Return the product of the first two arguments plus the third
 * argument.
 *
 * <p><i>Warning:</i> This does not delegate to the high-precision
 * platform-specific <code>fma()</code> implementation.
 *
 * @param x First argument.
 * @param y Second argument.
 * @param z Third argument.
 * @return The product of the first two arguments plus the third
 * argument.
 */
template <typename T1, typename T2, typename T3>
inline typename boost::math::tools::promote_args<T1, T2, T3>::type fma(
    const T1& x, const T2& y, const T3& z) {
  return x * y + z;
}

}  // namespace math
}  // namespace stan
#endif
