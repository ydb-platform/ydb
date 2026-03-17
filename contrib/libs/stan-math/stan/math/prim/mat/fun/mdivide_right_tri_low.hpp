#ifndef STAN_MATH_PRIM_MAT_FUN_MDIVIDE_RIGHT_TRI_LOW_HPP
#define STAN_MATH_PRIM_MAT_FUN_MDIVIDE_RIGHT_TRI_LOW_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/mdivide_right_tri.hpp>
#include <stan/math/prim/mat/fun/promote_common.hpp>

namespace stan {
namespace math {

/**
 * Returns the solution of the system tri(A)x=b when tri(A) is a
 * lower triangular view of the matrix A.
 * @param A Matrix.
 * @param b Right hand side matrix or vector.
 * @return x = b * tri(A)^-1, solution of the linear system.
 * @throws std::domain_error if A is not square or the rows of b don't
 * match the size of A.
 */
template <typename T1, typename T2, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<typename boost::math::tools::promote_args<T1, T2>::type,
                     R1, C2>
mdivide_right_tri_low(const Eigen::Matrix<T1, R1, C1> &b,
                      const Eigen::Matrix<T2, R2, C2> &A) {
  return mdivide_right_tri<Eigen::Lower>(
      promote_common<Eigen::Matrix<T1, R1, C1>, Eigen::Matrix<T2, R1, C1> >(b),
      promote_common<Eigen::Matrix<T1, R2, C2>, Eigen::Matrix<T2, R2, C2> >(A));
}

}  // namespace math
}  // namespace stan
#endif
