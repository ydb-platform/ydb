#ifndef STAN_MATH_PRIM_MAT_ERR_IS_UNIT_VECTOR_HPP
#define STAN_MATH_PRIM_MAT_ERR_IS_UNIT_VECTOR_HPP

#include <stan/math/prim/arr/err/is_nonzero_size.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {
/**
 * Return <code>true</code> if the vector is not a unit vector or if any
 * element is <code>NaN</code>.
 * A valid unit vector is one where the square elements
 * summed is equal to 1. This function tests that the sum
 * is within the tolerance specified by <code>CONSTRAINT_TOLERANCE</code>.
 * This function only accpets <code>Eigen::Matrix</code> vectors, statically
 * typed vectors, not general matrices with 1 column.
 * @tparam T_prob Scalar type of the vector, reqires class method
 *   <code>.squaredNorm()</code>
 * @param theta Eigen vector to test
 * @return <code>true</code> if the vector is not a unit
 *   vector or if any element is <code>NaN</code>
 */
template <typename T_prob>
inline bool is_unit_vector(
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  if (is_nonzero_size(theta)) {
    T_prob seq = theta.squaredNorm();
    return fabs(1.0 - seq) <= CONSTRAINT_TOLERANCE;
  }
  return false;
}

}  // namespace math
}  // namespace stan
#endif
