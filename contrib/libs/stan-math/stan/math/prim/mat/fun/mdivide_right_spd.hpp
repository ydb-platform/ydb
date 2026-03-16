#ifndef STAN_MATH_PRIM_MAT_FUN_MDIVIDE_RIGHT_SPD_HPP
#define STAN_MATH_PRIM_MAT_FUN_MDIVIDE_RIGHT_SPD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/mdivide_left_spd.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Returns the solution of the system Ax=b where A is symmetric
 * positive definite.
 * @param A Matrix.
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if A is not square or the rows of b don't
 * match the size of A.
 */
template <typename T1, typename T2, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<typename boost::math::tools::promote_args<T1, T2>::type,
                     R1, C2>
mdivide_right_spd(const Eigen::Matrix<T1, R1, C1> &b,
                  const Eigen::Matrix<T2, R2, C2> &A) {
  check_square("mdivide_right_spd", "A", A);
  check_multiplicable("mdivide_right_spd", "b", b, "A", A);
  check_symmetric("mdivide_right_spd", "A", A);
  check_pos_definite("mdivide_right_spd", "A", A);
  // FIXME: After allowing for general MatrixBase in mdivide_left_spd,
  //        change to b.transpose()
  return mdivide_left_spd(A, transpose(b)).transpose();
}

}  // namespace math
}  // namespace stan
#endif
