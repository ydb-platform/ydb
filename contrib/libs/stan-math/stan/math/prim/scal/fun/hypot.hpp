#ifndef STAN_MATH_PRIM_SCAL_FUN_HYPOT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_HYPOT_HPP

#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <boost/math/tools/promotion.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the length of the hypoteneuse of a right triangle with
 * opposite and adjacent side lengths given by the specified
 * arguments (C++11).  In symbols, if the arguments are
 * <code>x</code> and <code>y</code>, the result is <code>sqrt(x *
 * x + y * y)</code>.
 *
 * @param x First argument.
 * @param y Second argument.
 * @return Length of hypoteneuse of right triangle with opposite
 * and adjacent side lengths x and y.
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type hypot(
    const T1& x, const T2& y) {
  using std::sqrt;
  return sqrt(x * x + y * y);
}

}  // namespace math
}  // namespace stan
#endif
