#ifndef STAN_MATH_PRIM_MAT_PROB_POISSON_LOG_GLM_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_POISSON_LOG_GLM_LPMF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/mat/fun/value_of_rec.hpp>
#include <stan/math/prim/arr/fun/value_of_rec.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/mat/meta/is_vector.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/as_array_or_scalar.hpp>
#include <stan/math/prim/scal/meta/as_scalar.hpp>
#include <stan/math/prim/mat/meta/as_scalar.hpp>
#include <stan/math/prim/arr/meta/as_scalar.hpp>
#include <stan/math/prim/mat/meta/as_column_vector_or_scalar.hpp>
#include <stan/math/prim/scal/meta/as_column_vector_or_scalar.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Returns the log PMF of the Generalized Linear Model (GLM)
 * with Poisson distribution and log link function.
 * The idea is that poisson_log_glm_lpmf(y, x, alpha, beta) should
 * compute a more efficient version of poisson_log_lpmf(y, alpha + x * beta)
 * by using analytically simplified gradients.
 * If containers are supplied, returns the log sum of the probabilities.
 * @tparam T_y type of vector of variates (labels), integers >=0;
 * this can also be a single positive integer;
 * @tparam T_x type of the matrix of covariates (features); this
 * should be an Eigen::Matrix type whose number of rows should match the
 * length of y and whose number of columns should match the length of beta
 * @tparam T_alpha type of the intercept(s);
 * this can be a vector (of the same length as y) of intercepts or a single
 * value (for models with constant intercept);
 * @tparam T_beta type of the weight vector;
 * this can also be a single value;
 * @param y positive integer vector parameter
 * @param x design matrix
 * @param alpha intercept (in log odds)
 * @param beta weight vector
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if x, beta or alpha is infinite.
 * @throw std::domain_error if y is negative.
 * @throw std::invalid_argument if container sizes mismatch.
 */
template <bool propto, typename T_y, typename T_x, typename T_alpha,
          typename T_beta>
typename return_type<T_x, T_alpha, T_beta>::type poisson_log_glm_lpmf(
    const T_y& y, const T_x& x, const T_alpha& alpha, const T_beta& beta) {
  static const char* function = "poisson_log_glm_lpmf";
  typedef typename partials_return_type<T_y, T_x, T_alpha, T_beta>::type
      T_partials_return;
  typedef typename std::conditional<
      is_vector<T_alpha>::value,
      Eigen::Array<typename partials_return_type<T_alpha>::type, -1, 1>,
      typename partials_return_type<T_alpha>::type>::type T_alpha_val;

  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::exp;

  if (!(stan::length(y) && stan::length(x) && stan::length(beta)))
    return 0.0;

  T_partials_return logp(0.0);

  const size_t N = x.rows();
  const size_t M = x.cols();

  check_nonnegative(function, "Vector of dependent variables", y);
  check_consistent_size(function, "Vector of dependent variables", y, N);
  check_consistent_size(function, "Weight vector", beta, M);
  if (is_vector<T_alpha>::value)
    check_consistent_sizes(function, "Vector of intercepts", alpha,
                           "Vector of dependent variables", y);

  if (!include_summand<propto, T_x, T_alpha, T_beta>::value)
    return 0.0;

  const auto& x_val = value_of_rec(x);
  const auto& y_val = value_of_rec(y);
  const auto& beta_val = value_of_rec(beta);
  const auto& alpha_val = value_of_rec(alpha);

  const auto& y_val_vec = as_column_vector_or_scalar(y_val);
  const auto& beta_val_vec = as_column_vector_or_scalar(beta_val);
  const auto& alpha_val_vec = as_column_vector_or_scalar(alpha_val);

  Matrix<T_partials_return, Dynamic, 1> theta = x_val * beta_val_vec;
  theta.array() += as_array_or_scalar(alpha_val_vec);

  Matrix<T_partials_return, Dynamic, 1> theta_derivative
      = as_array_or_scalar(y_val_vec) - exp(theta.array());
  double theta_derivative_sum = sum(theta_derivative);
  if (!std::isfinite(theta_derivative_sum)) {
    check_finite(function, "Weight vector", beta);
    check_finite(function, "Intercept", alpha);
    check_finite(function, "Matrix of independent variables", theta);
  }
  if (include_summand<propto>::value) {
    if (is_vector<T_y>::value) {
      logp -= sum(lgamma(as_array_or_scalar(y_val_vec) + 1));
    } else {
      logp -= lgamma(as_scalar(y_val) + 1);
    }
  }
  if (include_summand<propto, T_partials_return>::value) {
    logp += sum(as_array_or_scalar(y_val_vec) * theta.array()
                - exp(theta.array()));
  }

  // Compute the necessary derivatives.
  operands_and_partials<T_x, T_alpha, T_beta> ops_partials(x, alpha, beta);
  if (!is_constant_struct<T_beta>::value) {
    ops_partials.edge3_.partials_ = x_val.transpose() * theta_derivative;
  }
  if (!is_constant_struct<T_x>::value) {
    ops_partials.edge1_.partials_
        = (beta_val_vec * theta_derivative.transpose()).transpose();
  }
  if (!is_constant_struct<T_alpha>::value) {
    if (is_vector<T_alpha>::value)
      ops_partials.edge2_.partials_ = theta_derivative;
    else
      ops_partials.edge2_.partials_[0] = theta_derivative_sum;
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_x, typename T_alpha, typename T_beta>
inline typename return_type<T_x, T_alpha, T_beta>::type poisson_log_glm_lpmf(
    const T_y& y, const T_x& x, const T_alpha& alpha, const T_beta& beta) {
  return poisson_log_glm_lpmf<false>(y, x, alpha, beta);
}
}  // namespace math
}  // namespace stan
#endif
