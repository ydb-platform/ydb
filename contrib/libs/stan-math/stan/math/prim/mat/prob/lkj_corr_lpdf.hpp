#ifndef STAN_MATH_PRIM_MAT_PROB_LKJ_CORR_LPDF_HPP
#define STAN_MATH_PRIM_MAT_PROB_LKJ_CORR_LPDF_HPP

#include <stan/math/prim/mat/err/check_corr_matrix.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/mat/fun/factor_cov_matrix.hpp>
#include <stan/math/prim/mat/fun/factor_U.hpp>
#include <stan/math/prim/mat/fun/read_corr_L.hpp>
#include <stan/math/prim/mat/fun/read_corr_matrix.hpp>
#include <stan/math/prim/mat/fun/read_cov_L.hpp>
#include <stan/math/prim/mat/fun/read_cov_matrix.hpp>
#include <stan/math/prim/mat/fun/make_nu.hpp>
#include <stan/math/prim/scal/fun/identity_constrain.hpp>
#include <stan/math/prim/scal/fun/identity_free.hpp>
#include <stan/math/prim/scal/fun/positive_constrain.hpp>
#include <stan/math/prim/scal/fun/positive_free.hpp>
#include <stan/math/prim/scal/fun/lb_constrain.hpp>
#include <stan/math/prim/scal/fun/lb_free.hpp>
#include <stan/math/prim/scal/fun/ub_constrain.hpp>
#include <stan/math/prim/scal/fun/ub_free.hpp>
#include <stan/math/prim/scal/fun/lub_constrain.hpp>
#include <stan/math/prim/scal/fun/lub_free.hpp>
#include <stan/math/prim/scal/fun/prob_constrain.hpp>
#include <stan/math/prim/scal/fun/prob_free.hpp>
#include <stan/math/prim/scal/fun/corr_constrain.hpp>
#include <stan/math/prim/scal/fun/corr_free.hpp>
#include <stan/math/prim/mat/fun/simplex_constrain.hpp>
#include <stan/math/prim/mat/fun/simplex_free.hpp>
#include <stan/math/prim/mat/fun/ordered_constrain.hpp>
#include <stan/math/prim/mat/fun/ordered_free.hpp>
#include <stan/math/prim/mat/fun/positive_ordered_constrain.hpp>
#include <stan/math/prim/mat/fun/positive_ordered_free.hpp>
#include <stan/math/prim/mat/fun/cholesky_factor_constrain.hpp>
#include <stan/math/prim/mat/fun/cholesky_factor_free.hpp>
#include <stan/math/prim/mat/fun/cholesky_corr_constrain.hpp>
#include <stan/math/prim/mat/fun/cholesky_corr_free.hpp>
#include <stan/math/prim/mat/fun/corr_matrix_constrain.hpp>
#include <stan/math/prim/mat/fun/corr_matrix_free.hpp>
#include <stan/math/prim/mat/fun/cov_matrix_constrain.hpp>
#include <stan/math/prim/mat/fun/cov_matrix_free.hpp>
#include <stan/math/prim/mat/fun/cov_matrix_constrain_lkj.hpp>
#include <stan/math/prim/mat/fun/cov_matrix_free_lkj.hpp>

namespace stan {
namespace math {

template <typename T_shape>
typename boost::math::tools::promote_args<double, T_shape>::type
do_lkj_constant(const T_shape& eta, const unsigned int& K) {
  // Lewandowski, Kurowicka, and Joe (2009) theorem 5
  typename boost::math::tools::promote_args<double, T_shape>::type constant;
  const int Km1 = K - 1;
  using stan::math::lgamma;
  if (eta == 1.0) {
    // C++ integer division is appropriate in this block
    Eigen::VectorXd denominator(Km1 / 2);
    for (int k = 1; k <= denominator.rows(); k++)
      denominator(k - 1) = lgamma(2.0 * k);
    constant = -denominator.sum();
    if ((K % 2) == 1)
      constant -= 0.25 * (K * K - 1) * LOG_PI - 0.25 * (Km1 * Km1) * LOG_TWO
                  - Km1 * lgamma(0.5 * (K + 1));
    else
      constant -= 0.25 * K * (K - 2) * LOG_PI
                  + 0.25 * (3 * K * K - 4 * K) * LOG_TWO + K * lgamma(0.5 * K)
                  - Km1 * lgamma(static_cast<double>(K));
  } else {
    constant = Km1 * lgamma(eta + 0.5 * Km1);
    for (int k = 1; k <= Km1; k++)
      constant -= 0.5 * k * LOG_PI + lgamma(eta + 0.5 * (Km1 - k));
  }
  return constant;
}

// LKJ_Corr(y|eta) [ y correlation matrix (not covariance matrix)
//                  eta > 0; eta == 1 <-> uniform]
template <bool propto, typename T_y, typename T_shape>
typename boost::math::tools::promote_args<T_y, T_shape>::type lkj_corr_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const T_shape& eta) {
  static const char* function = "lkj_corr_lpdf";

  using boost::math::tools::promote_args;

  typename promote_args<T_y, T_shape>::type lp(0.0);
  check_positive(function, "Shape parameter", eta);
  check_corr_matrix(function, "Correlation matrix", y);

  const unsigned int K = y.rows();
  if (K == 0)
    return 0.0;

  if (include_summand<propto, T_shape>::value)
    lp += do_lkj_constant(eta, K);

  if ((eta == 1.0)
      && stan::is_constant<typename stan::scalar_type<T_shape> >::value)
    return lp;

  if (!include_summand<propto, T_y, T_shape>::value)
    return lp;

  Eigen::Matrix<T_y, Eigen::Dynamic, 1> values
      = y.ldlt().vectorD().array().log().matrix();
  lp += (eta - 1.0) * sum(values);
  return lp;
}

template <typename T_y, typename T_shape>
inline typename boost::math::tools::promote_args<T_y, T_shape>::type
lkj_corr_lpdf(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
              const T_shape& eta) {
  return lkj_corr_lpdf<false>(y, eta);
}

}  // namespace math
}  // namespace stan
#endif
