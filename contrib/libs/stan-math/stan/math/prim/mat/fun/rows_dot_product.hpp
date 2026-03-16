#ifndef STAN_MATH_PRIM_MAT_FUN_ROWS_DOT_PRODUCT_HPP
#define STAN_MATH_PRIM_MAT_FUN_ROWS_DOT_PRODUCT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>

namespace stan {
namespace math {

/**
 * Returns the dot product of the specified vectors.
 *
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<double, R1, 1> rows_dot_product(
    const Eigen::Matrix<double, R1, C1>& v1,
    const Eigen::Matrix<double, R2, C2>& v2) {
  check_matching_sizes("rows_dot_product", "v1", v1, "v2", v2);
  return (v1.cwiseProduct(v2)).rowwise().sum();
}

}  // namespace math
}  // namespace stan
#endif
