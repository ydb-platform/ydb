#ifndef STAN_MATH_PRIM_MAT_PROB_MULTI_GP_CHOLESKY_LPDF_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTI_GP_CHOLESKY_LPDF_HPP

#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/mat/fun/dot_self.hpp>
#include <stan/math/prim/mat/fun/log.hpp>
#include <stan/math/prim/mat/fun/mdivide_left_tri_low.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/row.hpp>
#include <stan/math/prim/mat/fun/sum.hpp>

namespace stan {
namespace math {
/**
 * The log of a multivariate Gaussian Process for the given y, w, and
 * a Cholesky factor L of the kernel matrix Sigma.
 * Sigma = LL', a square, semi-positive definite matrix.
 * y is a dxN matrix, where each column is a different observation and each
 * row is a different output dimension.  The Gaussian Process is assumed to
 * have a scaled kernel matrix with a different scale for each output dimension.
 * This distribution is equivalent to:
 *    for (i in 1:d) row(y, i) ~ multi_normal(0, (1/w[i])*LL').
 *
 * @param y A dxN matrix
 * @param L The Cholesky decomposition of a kernel matrix
 * @param w A d-dimensional vector of positve inverse scale parameters for each
 * output.
 * @return The log of the multivariate GP density.
 * @throw std::domain_error if Sigma is not square, not symmetric,
 * or not semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_covar Type of kernel.
 * @tparam T_w Type of weight.
 */
template <bool propto, typename T_y, typename T_covar, typename T_w>
typename boost::math::tools::promote_args<T_y, T_covar, T_w>::type
multi_gp_cholesky_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const Eigen::Matrix<T_w, Eigen::Dynamic, 1>& w) {
  static const char* function = "multi_gp_cholesky_lpdf";
  typedef
      typename boost::math::tools::promote_args<T_y, T_covar, T_w>::type T_lp;
  T_lp lp(0.0);

  check_size_match(function, "Size of random variable (rows y)", y.rows(),
                   "Size of kernel scales (w)", w.size());
  check_size_match(function, "Size of random variable", y.cols(),
                   "rows of covariance parameter", L.rows());
  check_finite(function, "Kernel scales", w);
  check_positive(function, "Kernel scales", w);
  check_finite(function, "Random variable", y);

  if (y.rows() == 0)
    return lp;

  if (include_summand<propto>::value) {
    lp += NEG_LOG_SQRT_TWO_PI * y.rows() * y.cols();
  }

  if (include_summand<propto, T_covar>::value) {
    lp -= L.diagonal().array().log().sum() * y.rows();
  }

  if (include_summand<propto, T_w>::value) {
    lp += 0.5 * y.cols() * sum(log(w));
  }

  if (include_summand<propto, T_y, T_w, T_covar>::value) {
    T_lp sum_lp_vec(0.0);
    for (int i = 0; i < y.rows(); i++) {
      Eigen::Matrix<T_y, Eigen::Dynamic, 1> y_row(y.row(i));
      Eigen::Matrix<
          typename boost::math::tools::promote_args<T_y, T_covar>::type,
          Eigen::Dynamic, 1>
          half(mdivide_left_tri_low(L, y_row));
      sum_lp_vec += w(i) * dot_self(half);
    }
    lp -= 0.5 * sum_lp_vec;
  }

  return lp;
}

template <typename T_y, typename T_covar, typename T_w>
inline typename boost::math::tools::promote_args<T_y, T_covar, T_w>::type
multi_gp_cholesky_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const Eigen::Matrix<T_w, Eigen::Dynamic, 1>& w) {
  return multi_gp_cholesky_lpdf<false>(y, L, w);
}

}  // namespace math
}  // namespace stan
#endif
