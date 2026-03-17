#ifndef STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_CONSTRAIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_CONSTRAIN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/multiply_lower_tri_self_transpose.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <cmath>

namespace stan {

namespace math {

/**
 * Return the symmetric, positive-definite matrix of dimensions K
 * by K resulting from transforming the specified finite vector of
 * size K plus (K choose 2).
 *
 * <p>See <code>cov_matrix_free()</code> for the inverse transform.
 *
 * @param x The vector to convert to a covariance matrix.
 * @param K The number of rows and columns of the resulting
 * covariance matrix.
 * @throws std::invalid_argument if (x.size() != K + (K choose 2)).
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cov_matrix_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x,
    typename math::index_type<Eigen::Matrix<T, Eigen::Dynamic, 1> >::type K) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::exp;
  typedef typename index_type<Matrix<T, Dynamic, Dynamic> >::type index_t;

  Matrix<T, Dynamic, Dynamic> L(K, K);
  check_size_match("cov_matrix_constrain", "x.size()", x.size(),
                   "K + (K choose 2)", (K * (K + 1)) / 2);
  int i = 0;
  for (index_t m = 0; m < K; ++m) {
    for (int n = 0; n < m; ++n)
      L(m, n) = x(i++);
    L(m, m) = exp(x(i++));
    for (index_t n = m + 1; n < K; ++n)
      L(m, n) = 0.0;
  }
  return multiply_lower_tri_self_transpose(L);
}

/**
 * Return the symmetric, positive-definite matrix of dimensions K
 * by K resulting from transforming the specified finite vector of
 * size K plus (K choose 2).
 *
 * <p>See <code>cov_matrix_free()</code> for the inverse transform.
 *
 * @param x The vector to convert to a covariance matrix.
 * @param K The dimensions of the resulting covariance matrix.
 * @param lp Reference
 * @throws std::domain_error if (x.size() != K + (K choose 2)).
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cov_matrix_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x,
    typename math::index_type<
        Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> >::type K,
    T& lp) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::exp;
  using std::log;
  typedef typename index_type<Matrix<T, Dynamic, Dynamic> >::type index_t;
  check_size_match("cov_matrix_constrain", "x.size()", x.size(),
                   "K + (K choose 2)", (K * (K + 1)) / 2);
  Matrix<T, Dynamic, Dynamic> L(K, K);
  int i = 0;
  for (index_t m = 0; m < K; ++m) {
    for (index_t n = 0; n < m; ++n)
      L(m, n) = x(i++);
    L(m, m) = exp(x(i++));
    for (index_t n = m + 1; n < K; ++n)
      L(m, n) = 0.0;
  }
  // Jacobian for complete transform, including exp() above
  lp += (K * LOG_2);  // needless constant; want propto
  for (index_t k = 0; k < K; ++k)
    lp += (K - k + 1) * log(L(k, k));  // only +1 because index from 0
  return multiply_lower_tri_self_transpose(L);
}

}  // namespace math
}  // namespace stan
#endif
