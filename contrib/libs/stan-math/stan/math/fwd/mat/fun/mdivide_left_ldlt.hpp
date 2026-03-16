#ifndef STAN_MATH_FWD_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP
#define STAN_MATH_FWD_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/LDLT_factor.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/fun/promote_common.hpp>
#include <stan/math/prim/mat/fun/mdivide_left_ldlt.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Returns the solution of the system Ax=b given an LDLT_factor of A
 * @param A LDLT_factor
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if rows of b don't match the size of A.
 */

template <int R1, int C1, int R2, int C2, typename T2>
inline Eigen::Matrix<fvar<T2>, R1, C2> mdivide_left_ldlt(
    const LDLT_factor<double, R1, C1> &A,
    const Eigen::Matrix<fvar<T2>, R2, C2> &b) {
  check_multiplicable("mdivide_left_ldlt", "A", A, "b", b);

  Eigen::Matrix<T2, R2, C2> b_val(b.rows(), b.cols());
  Eigen::Matrix<T2, R2, C2> b_der(b.rows(), b.cols());
  for (int i = 0; i < b.rows(); i++)
    for (int j = 0; j < b.cols(); j++) {
      b_val(i, j) = b(i, j).val_;
      b_der(i, j) = b(i, j).d_;
    }

  return to_fvar(mdivide_left_ldlt(A, b_val), mdivide_left_ldlt(A, b_der));
}

}  // namespace math
}  // namespace stan
#endif
