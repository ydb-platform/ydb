#ifndef STAN_MATH_PRIM_MAT_FUN_PROD_HPP
#define STAN_MATH_PRIM_MAT_FUN_PROD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the product of the coefficients of the specified
 * standard vector.
 * @param v Specified vector.
 * @return Product of coefficients of vector.
 */
template <typename T>
inline T prod(const std::vector<T>& v) {
  if (v.size() == 0)
    return 1;
  Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>> m(&v[0], v.size());
  return m.prod();
}

/**
 * Returns the product of the coefficients of the specified
 * column vector.
 * @param v Specified vector.
 * @return Product of coefficients of vector.
 */
template <typename T, int R, int C>
inline T prod(const Eigen::Matrix<T, R, C>& v) {
  if (v.size() == 0)
    return 1.0;
  return v.prod();
}

}  // namespace math
}  // namespace stan
#endif
