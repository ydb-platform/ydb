#ifndef STAN_MATH_PRIM_MAT_FUN_UNIT_VECTOR_FREE_HPP
#define STAN_MATH_PRIM_MAT_FUN_UNIT_VECTOR_FREE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/mat/err/check_unit_vector.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Transformation of a unit length vector to a "free" vector
 * However, we are just fixing the unidentified radius to 1.
 * Thus, the transformation is just the identity
 *
 * @param x unit vector of dimension K
 * @return Unit vector of dimension K considered "free"
 * @tparam T Scalar type.
 **/
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> unit_vector_free(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x) {
  check_unit_vector("stan::math::unit_vector_free", "Unit vector variable", x);
  return x;
}

}  // namespace math
}  // namespace stan
#endif
