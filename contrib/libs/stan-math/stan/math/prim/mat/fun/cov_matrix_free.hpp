#ifndef STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_FREE_HPP
#define STAN_MATH_PRIM_MAT_FUN_COV_MATRIX_FREE_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * The covariance matrix derived from the symmetric view of the
 * lower-triangular view of the K by K specified matrix is freed
 * to return a vector of size K + (K choose 2).
 *
 * This is the inverse of the <code>cov_matrix_constrain()</code>
 * function so that for any finite vector <code>x</code> of size K
 * + (K choose 2),
 *
 * <code>x == cov_matrix_free(cov_matrix_constrain(x, K))</code>.
 *
 * In order for this round-trip to work (and really for this
 * function to work), the symmetric view of its lower-triangular
 * view must be positive definite.
 *
 * @param y Matrix of dimensions K by K such that he symmetric
 * view of the lower-triangular view is positive definite.
 * @return Vector of size K plus (K choose 2) in (-inf, inf)
 * that produces
 * @throw std::domain_error if <code>y</code> is not square,
 * has zero dimensionality, or has a non-positive diagonal element.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> cov_matrix_free(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_square("cov_matrix_free", "y", y);
  check_nonzero_size("cov_matrix_free", "y", y);

  using std::log;
  int K = y.rows();
  for (int k = 0; k < K; ++k)
    check_positive("cov_matrix_free", "y", y(k, k));
  Eigen::Matrix<T, Eigen::Dynamic, 1> x((K * (K + 1)) / 2);
  // FIXME: see Eigen LDLT for rank-revealing version -- use that
  // even if less efficient?
  Eigen::LLT<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> > llt(y.rows());
  llt.compute(y);
  Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> L = llt.matrixL();
  int i = 0;
  for (int m = 0; m < K; ++m) {
    for (int n = 0; n < m; ++n)
      x(i++) = L(m, n);
    x(i++) = log(L(m, m));
  }
  return x;
}

}  // namespace math
}  // namespace stan
#endif
