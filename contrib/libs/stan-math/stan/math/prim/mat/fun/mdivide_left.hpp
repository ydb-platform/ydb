#ifndef STAN_MATH_PRIM_MAT_FUN_MDIVIDE_LEFT_HPP
#define STAN_MATH_PRIM_MAT_FUN_MDIVIDE_LEFT_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/promote_common.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>

namespace stan {
namespace math {

/**
 * Returns the solution of the system Ax=b.
 * @param A Matrix.
 * @param b Right hand side matrix or vector.
 * @return x = A^-1 b, solution of the linear system.
 * @throws std::domain_error if A is not square or the rows of b don't
 * match the size of A.
 */
template <typename T1, typename T2, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<typename boost::math::tools::promote_args<T1, T2>::type,
                     R1, C2>
mdivide_left(const Eigen::Matrix<T1, R1, C1> &A,
             const Eigen::Matrix<T2, R2, C2> &b) {
  check_square("mdivide_left", "A", A);
  check_multiplicable("mdivide_left", "A", A, "b", b);
  return promote_common<Eigen::Matrix<T1, R1, C1>, Eigen::Matrix<T2, R1, C1> >(
             A)
      .lu()
      .solve(
          promote_common<Eigen::Matrix<T1, R2, C2>, Eigen::Matrix<T2, R2, C2> >(
              b));
}

}  // namespace math
}  // namespace stan
#endif
