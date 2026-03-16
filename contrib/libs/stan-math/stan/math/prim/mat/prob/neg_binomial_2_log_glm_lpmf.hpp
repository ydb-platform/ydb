#ifndef STAN_MATH_PRIM_MAT_PROB_NEG_BINOMIAL_2_LOG_GLM_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_NEG_BINOMIAL_2_LOG_GLM_LPMF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/mat/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/log_sum_exp.hpp>
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
#include <stan/math/prim/scal/fun/sum.hpp>
#include <vector>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the log PMF of the Generalized Linear Model (GLM)
 * with Negative-Binomial-2 distribution and log link function.
 * The idea is that neg_binomial_2_log_glm_lpmf(y, x, alpha, beta, phi) should
 * compute a more efficient version of
 * neg_binomial_2_log_lpmf(y, alpha + x * beta, phi) by using analytically
 * simplified gradients.
 * If containers are supplied, returns the log sum of the probabilities.
 * @tparam T_y type of positive int vector of variates (labels);
 * this can also be a single positive integer value;
 * @tparam T_x type of the matrix of covariates (features); this
 * should be an Eigen::Matrix type whose number of rows should match the
 * length of y and whose number of columns should match the length of beta
 * @tparam T_alpha type of the intercept(s);
 * this can be a vector (of the same length as y) of intercepts or a single
 * value (for models with constant intercept);
 * @tparam T_beta type of the weight vector;
 * this can also be a scalar;
 * @tparam T_precision type of the (positive) precision(s);
 * this can be a vector (of the same length as y, for heteroskedasticity)
 * or a scalar.
 * @param y failures count vector parameter
 * @param x design matrix
 * @param alpha intercept (in log odds)
 * @param beta weight vector
 * @param phi (vector of) precision parameter(s)
 * @return log probability or log sum of probabilities
 * @throw std::invalid_argument if container sizes mismatch.
 * @throw std::domain_error if x, beta or alpha is infinite.
 * @throw std::domain_error if phi is infinite or non-positive.
 * @throw std::domain_error if y is negative.
 */
template <bool propto, typename T_y, typename T_x, typename T_alpha,
          typename T_beta, typename T_precision>
typename return_type<T_x, T_alpha, T_beta, T_precision>::type
neg_binomial_2_log_glm_lpmf(const T_y& y, const T_x& x, const T_alpha& alpha,
                            const T_beta& beta, const T_precision& phi) {
  static const char* function = "neg_binomial_2_log_glm_lpmf";
  typedef
      typename stan::partials_return_type<T_y, T_x, T_alpha, T_beta,
                                          T_precision>::type T_partials_return;
  typedef typename std::conditional<
      is_vector<T_precision>::value,
      Eigen::Array<typename partials_return_type<T_precision>::type, -1, 1>,
      typename partials_return_type<T_precision>::type>::type T_precision_val;
  typedef typename std::conditional<
      is_vector<T_y>::value || is_vector<T_precision>::value,
      Eigen::Array<typename partials_return_type<T_y, T_precision>::type, -1,
                   1>,
      typename partials_return_type<T_y, T_precision>::type>::type T_sum_val;

  using Eigen::Array;
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using Eigen::exp;
  using Eigen::log1p;

  if (!(stan::length(y) && stan::length(x) && stan::length(beta)
        && stan::length(phi)))
    return 0.0;

  T_partials_return logp(0.0);

  const size_t N = x.rows();
  const size_t M = x.cols();

  check_nonnegative(function, "Failures variables", y);
  check_finite(function, "Weight vector", beta);
  check_finite(function, "Intercept", alpha);
  check_positive_finite(function, "Precision parameter", phi);
  check_consistent_size(function, "Vector of dependent variables", y, N);
  check_consistent_size(function, "Weight vector", beta, M);
  if (is_vector<T_precision>::value)
    check_consistent_sizes(function, "Vector of precision parameters", phi,
                           "Vector of dependent variables", y);
  if (is_vector<T_alpha>::value)
    check_consistent_sizes(function, "Vector of intercepts", alpha,
                           "Vector of dependent variables", y);

  if (!include_summand<propto, T_x, T_alpha, T_beta, T_precision>::value)
    return 0.0;

  const auto& x_val = value_of_rec(x);
  const auto& y_val = value_of_rec(y);
  const auto& beta_val = value_of_rec(beta);
  const auto& alpha_val = value_of_rec(alpha);
  const auto& phi_val = value_of_rec(phi);

  const auto& y_val_vec = as_column_vector_or_scalar(y_val);
  const auto& beta_val_vec = as_column_vector_or_scalar(beta_val);
  const auto& alpha_val_vec = as_column_vector_or_scalar(alpha_val);
  const auto& phi_val_vec = as_column_vector_or_scalar(phi_val);

  const auto& y_arr = as_array_or_scalar(y_val_vec);
  const auto& phi_arr = as_array_or_scalar(phi_val_vec);

  Array<T_partials_return, Dynamic, 1> theta = value_of(x) * beta_val_vec;
  theta += as_array_or_scalar(alpha_val_vec);
  check_finite(function, "Matrix of independent variables", theta);
  T_precision_val log_phi = log(phi_arr);
  Array<T_partials_return, Dynamic, 1> logsumexp_theta_logphi
      = (theta > log_phi)
            .select(theta + log1p(exp(log_phi - theta)),
                    log_phi + log1p(exp(theta - log_phi)));

  T_sum_val y_plus_phi = y_arr + phi_arr;

  // Compute the log-density.
  if (include_summand<propto>::value) {
    logp -= sum(lgamma(y_arr + 1));
  }
  if (include_summand<propto, T_precision>::value) {
    if (is_vector<T_precision>::value) {
      scalar_seq_view<decltype(phi_val)> phi_vec(phi_val);
      for (size_t n = 0; n < N; ++n)
        logp += multiply_log(phi_vec[n], phi_vec[n]) - lgamma(phi_vec[n]);
    } else {
      logp += N
              * (multiply_log(as_scalar(phi_val), as_scalar(phi_val))
                 - lgamma(as_scalar(phi_val)));
    }
  }
  if (include_summand<propto, T_x, T_alpha, T_beta, T_precision>::value)
    logp -= sum(y_plus_phi * logsumexp_theta_logphi);
  if (include_summand<propto, T_x, T_alpha, T_beta>::value)
    logp += sum(y_arr * theta);
  if (include_summand<propto, T_precision>::value) {
    logp += sum(lgamma(y_plus_phi));
  }

  // Compute the necessary derivatives.
  operands_and_partials<T_x, T_alpha, T_beta, T_precision> ops_partials(
      x, alpha, beta, phi);
  if (!(is_constant_struct<T_x>::value && is_constant_struct<T_beta>::value
        && is_constant_struct<T_alpha>::value
        && is_constant_struct<T_precision>::value)) {
    Array<T_partials_return, Dynamic, 1> theta_exp = theta.exp();
    if (!(is_constant_struct<T_x>::value && is_constant_struct<T_beta>::value
          && is_constant_struct<T_alpha>::value)) {
      Matrix<T_partials_return, Dynamic, 1> theta_derivative
          = y_arr - theta_exp * y_plus_phi / (theta_exp + phi_arr);
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
          ops_partials.edge2_.partials_[0] = sum(theta_derivative);
      }
    }
    if (!is_constant_struct<T_precision>::value) {
      if (is_vector<T_precision>::value) {
        ops_partials.edge4_.partials_
            = 1 - y_plus_phi / (theta_exp + phi_arr) + log_phi
              - logsumexp_theta_logphi + as_array_or_scalar(digamma(y_plus_phi))
              - as_array_or_scalar(digamma(phi_val_vec));
      } else {
        ops_partials.edge4_.partials_[0]
            = N
              + sum(-y_plus_phi / (theta_exp + phi_arr) + log_phi
                    - logsumexp_theta_logphi
                    + as_array_or_scalar(digamma(y_plus_phi))
                    - as_array_or_scalar(digamma(phi_val_vec)));
      }
    }
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_x, typename T_alpha, typename T_beta,
          typename T_precision>
inline typename return_type<T_x, T_alpha, T_beta, T_precision>::type
neg_binomial_2_log_glm_lpmf(const T_y& y, const T_x& x, const T_alpha& alpha,
                            const T_beta& beta, const T_precision& phi) {
  return neg_binomial_2_log_glm_lpmf<false>(y, x, alpha, beta, phi);
}
}  // namespace math
}  // namespace stan
#endif
