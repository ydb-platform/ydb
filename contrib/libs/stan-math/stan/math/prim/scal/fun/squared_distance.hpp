#ifndef STAN_MATH_PRIM_SCAL_FUN_SQUARED_DISTANCE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SQUARED_DISTANCE_HPP

#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/fun/square.hpp>

namespace stan {
namespace math {

/**
 * Returns the squared distance.
 *
 * @param x1 First vector.
 * @param x2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type squared_distance(
    const T1& x1, const T2& x2) {
  check_finite("squared_distance", "x1", x1);
  check_finite("squared_distance", "x2", x2);
  return square(x1 - x2);
}
}  // namespace math
}  // namespace stan
#endif
