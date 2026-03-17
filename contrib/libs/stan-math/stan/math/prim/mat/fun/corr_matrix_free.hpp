#ifndef STAN_MATH_PRIM_MAT_FUN_CORR_MATRIX_FREE_HPP
#define STAN_MATH_PRIM_MAT_FUN_CORR_MATRIX_FREE_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/fun/factor_cov_matrix.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the vector of unconstrained partial correlations that
 * define the specified correlation matrix when transformed.
 *
 * <p>The constraining transform is defined as for
 * <code>corr_matrix_constrain(Matrix, size_t)</code>.  The
 * inverse transform in this function is simpler in that it only
 * needs to compute the \f$k \choose 2\f$ partial correlations
 * and then free those.
 *
 * @param y The correlation matrix to free.
 * @return Vector of unconstrained values that produce the
 * specified correlation matrix when transformed.
 * @tparam T Type of scalar.
 * @throw std::domain_error if the correlation matrix has no
 *    elements or is not a square matrix.
 * @throw std::runtime_error if the correlation matrix cannot be
 *    factorized by factor_cov_matrix() or if the sds returned by
 *    factor_cov_matrix() on log scale are unconstrained.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> corr_matrix_free(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_square("corr_matrix_free", "y", y);
  check_nonzero_size("corr_matrix_free", "y", y);

  using Eigen::Array;
  using Eigen::Dynamic;
  using Eigen::Matrix;
  typedef typename index_type<Matrix<T, Dynamic, 1> >::type size_type;

  size_type k = y.rows();
  size_type k_choose_2 = (k * (k - 1)) / 2;
  Array<T, Dynamic, 1> x(k_choose_2);
  Array<T, Dynamic, 1> sds(k);
  bool successful = factor_cov_matrix(y, x, sds);
  if (!successful)
    domain_error("corr_matrix_free", "factor_cov_matrix failed on y", y, "");
  for (size_type i = 0; i < k; ++i) {
    check_bounded("corr_matrix_free", "log(sd)", sds[i], -CONSTRAINT_TOLERANCE,
                  CONSTRAINT_TOLERANCE);
  }
  return x.matrix();
}
}  // namespace math
}  // namespace stan
#endif
