#ifndef STAN_MATH_PRIM_MAT_FUN_INVERSE_SPD_HPP
#define STAN_MATH_PRIM_MAT_FUN_INVERSE_SPD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>

namespace stan {
namespace math {

/**
 * Returns the inverse of the specified symmetric, pos/neg-definite matrix.
 * @param m Specified matrix.
 * @return Inverse of the matrix.
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> inverse_spd(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m) {
  using Eigen::Dynamic;
  using Eigen::LDLT;
  using Eigen::Matrix;
  check_square("inverse_spd", "m", m);
  check_symmetric("inverse_spd", "m", m);
  Matrix<T, Dynamic, Dynamic> mmt = T(0.5) * (m + m.transpose());
  LDLT<Matrix<T, Dynamic, Dynamic> > ldlt(mmt);
  if (ldlt.info() != Eigen::Success)
    domain_error("invese_spd", "LDLT factor failed", "", "");
  if (!ldlt.isPositive())
    domain_error("invese_spd", "matrix not positive definite", "", "");
  Matrix<T, Dynamic, 1> diag_ldlt = ldlt.vectorD();
  for (int i = 0; i < diag_ldlt.size(); ++i)
    if (diag_ldlt(i) <= 0)
      domain_error("invese_spd", "matrix not positive definite", "", "");
  return ldlt.solve(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>::Identity(
      m.rows(), m.cols()));
}

}  // namespace math
}  // namespace stan
#endif
