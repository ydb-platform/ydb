#ifndef STAN_MATH_PRIM_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP
#define STAN_MATH_PRIM_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/LDLT_factor.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/fun/promote_common.hpp>
#include <type_traits>

namespace stan {
namespace math {

/**
 * Returns the solution of the system Ax=b given an LDLT_factor of A
 * @param A LDLT_factor
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if rows of b don't match the size of A.
 */

template <int R1, int C1, int R2, int C2, typename T1, typename T2>
inline Eigen::Matrix<typename boost::math::tools::promote_args<T1, T2>::type,
                     R1, C2>
mdivide_left_ldlt(const LDLT_factor<T1, R1, C1> &A,
                  const Eigen::Matrix<T2, R2, C2> &b) {
  check_multiplicable("mdivide_left_ldlt", "A", A, "b", b);

  return A.solve(
      promote_common<Eigen::Matrix<T1, R2, C2>, Eigen::Matrix<T2, R2, C2> >(b));
}

}  // namespace math
}  // namespace stan
#endif
