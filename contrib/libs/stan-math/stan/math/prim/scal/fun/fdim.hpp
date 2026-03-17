#ifndef STAN_MATH_PRIM_SCAL_FUN_FDIM_HPP
#define STAN_MATH_PRIM_SCAL_FUN_FDIM_HPP

#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <boost/math/tools/promotion.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the positive difference of the specified values (C++11).
 *
 * The function is defined by
 *
 * <code>fdim(x, y) = (x > y) ? (x - y) : 0</code>.
 *
 * @param x First value.
 * @param y Second value.
 * @return max(x- y, 0)
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type fdim(T1 x,
                                                                    T2 y) {
  typedef typename boost::math::tools::promote_args<T1, T2>::type return_t;
  using std::numeric_limits;
  if (is_nan(x) || is_nan(y))
    return numeric_limits<return_t>::quiet_NaN();
  return (x <= y) ? 0 : x - y;
}

}  // namespace math
}  // namespace stan
#endif
