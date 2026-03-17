#ifndef STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_CONSTRAIN_LKJ_HPP
#define STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_CONSTRAIN_LKJ_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/fun/corr_constrain.hpp>
#include <stan/math/prim/scal/fun/positive_constrain.hpp>
#include <stan/math/prim/mat/fun/read_cov_matrix.hpp>

namespace stan {
namespace math {

/**
 * Return the covariance matrix of the specified dimensionality
 * derived from constraining the specified vector of unconstrained
 * values.  The input vector must be of length \f$k \choose 2 +
 * k\f$.  The first \f$k \choose 2\f$ values in the input
 * represent unconstrained (partial) correlations and the last
 * \f$k\f$ are unconstrained standard deviations of the dimensions.
 *
 * <p>The transform scales the correlation matrix transform defined
 * in <code>corr_matrix_constrain(Matrix, size_t)</code>
 * with the constrained deviations.
 *
 * @param x Input vector of unconstrained partial correlations and
 * standard deviations.
 * @param k Dimensionality of returned covariance matrix.
 * @return Covariance matrix derived from the unconstrained partial
 * correlations and deviations.
 * @tparam T Type of scalar.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cov_matrix_constrain_lkj(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, size_t k) {
  size_t k_choose_2 = (k * (k - 1)) / 2;
  Eigen::Array<T, Eigen::Dynamic, 1> cpcs(k_choose_2);
  int pos = 0;
  for (size_t i = 0; i < k_choose_2; ++i)
    cpcs[i] = corr_constrain(x[pos++]);
  Eigen::Array<T, Eigen::Dynamic, 1> sds(k);
  for (size_t i = 0; i < k; ++i)
    sds[i] = positive_constrain(x[pos++]);
  return read_cov_matrix(cpcs, sds);
}

/**
 * Return the covariance matrix of the specified dimensionality
 * derived from constraining the specified vector of unconstrained
 * values and increment the specified log probability reference
 * with the log absolute Jacobian determinant.
 *
 * <p>The transform is defined as for
 * <code>cov_matrix_constrain(Matrix, size_t)</code>.
 *
 * <p>The log absolute Jacobian determinant is derived by
 * composing the log absolute Jacobian determinant for the
 * underlying correlation matrix as defined in
 * <code>cov_matrix_constrain(Matrix, size_t, T&)</code> with
 * the Jacobian of the transfrom of the correlation matrix
 * into a covariance matrix by scaling by standard deviations.
 *
 * @param x Input vector of unconstrained partial correlations and
 * standard deviations.
 * @param k Dimensionality of returned covariance matrix.
 * @param lp Log probability reference to increment.
 * @return Covariance matrix derived from the unconstrained partial
 * correlations and deviations.
 * @tparam T Type of scalar.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cov_matrix_constrain_lkj(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, size_t k, T& lp) {
  size_t k_choose_2 = (k * (k - 1)) / 2;
  Eigen::Array<T, Eigen::Dynamic, 1> cpcs(k_choose_2);
  int pos = 0;
  for (size_t i = 0; i < k_choose_2; ++i)
    cpcs[i] = corr_constrain(x[pos++], lp);
  Eigen::Array<T, Eigen::Dynamic, 1> sds(k);
  for (size_t i = 0; i < k; ++i)
    sds[i] = positive_constrain(x[pos++], lp);
  return read_cov_matrix(cpcs, sds, lp);
}

}  // namespace math

}  // namespace stan

#endif
