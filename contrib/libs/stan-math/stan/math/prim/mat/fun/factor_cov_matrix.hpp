#ifndef STAN_MATH_PRIM_MAT_FUN_FACTOR_COV_MATRIX_HPP
#define STAN_MATH_PRIM_MAT_FUN_FACTOR_COV_MATRIX_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/factor_U.hpp>
#include <cstddef>

namespace stan {
namespace math {

/**
 * This function is intended to make starting values, given a
 * covariance matrix Sigma
 *
 * The transformations are hard coded as log for standard
 * deviations and Fisher transformations (atanh()) of CPCs
 *
 * @param[in] Sigma covariance matrix
 * @param[out] CPCs fill this unbounded (does not resize)
 * @param[out] sds fill this unbounded (does not resize)
 * @return false if any of the diagonals of Sigma are 0
 */
template <typename T>
bool factor_cov_matrix(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& Sigma,
    Eigen::Array<T, Eigen::Dynamic, 1>& CPCs,
    Eigen::Array<T, Eigen::Dynamic, 1>& sds) {
  size_t K = sds.rows();

  sds = Sigma.diagonal().array();
  if ((sds <= 0.0).any())
    return false;
  sds = sds.sqrt();

  Eigen::DiagonalMatrix<T, Eigen::Dynamic> D(K);
  D.diagonal() = sds.inverse();
  sds = sds.log();  // now unbounded

  Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> R = D * Sigma * D;
  // to hopefully prevent pivoting due to floating point error
  R.diagonal().setOnes();
  Eigen::LDLT<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> > ldlt;
  ldlt = R.ldlt();
  if (!ldlt.isPositive())
    return false;
  Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> U = ldlt.matrixU();
  factor_U(U, CPCs);
  return true;
}

}  // namespace math

}  // namespace stan

#endif
