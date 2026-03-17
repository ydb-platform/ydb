#ifndef STAN_MATH_PRIM_MAT_PROB_GAUSSIAN_DLM_OBS_LPDF_HPP
#define STAN_MATH_PRIM_MAT_PROB_GAUSSIAN_DLM_OBS_LPDF_HPP

#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/mat/err/check_spsd_matrix.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/mat/fun/add.hpp>
#include <stan/math/prim/mat/fun/dot_product.hpp>
#include <stan/math/prim/mat/fun/inverse_spd.hpp>
#include <stan/math/prim/mat/fun/log.hpp>
#include <stan/math/prim/mat/fun/log_determinant_spd.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/quad_form.hpp>
#include <stan/math/prim/mat/fun/quad_form_sym.hpp>
#include <stan/math/prim/mat/fun/subtract.hpp>
#include <stan/math/prim/mat/fun/tcrossprod.hpp>
#include <stan/math/prim/mat/fun/trace_quad_form.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>

/*
  TODO: time-varying system matrices
  TODO: use sequential processing even for non-diagonal obs
  covariance.
  TODO: add constant terms in observation.
*/
namespace stan {
namespace math {
/**
 * The log of a Gaussian dynamic linear model (GDLM).
 * This distribution is equivalent to, for \f$t = 1:T\f$,
 * \f{eqnarray*}{
 * y_t & \sim N(F' \theta_t, V) \\
 * \theta_t & \sim N(G \theta_{t-1}, W) \\
 * \theta_0 & \sim N(m_0, C_0)
 * \f}
 *
 * If V is a vector, then the Kalman filter is applied
 * sequentially.
 *
 * @param y A r x T matrix of observations. Rows are variables,
 * columns are observations.
 * @param F A n x r matrix. The design matrix.
 * @param G A n x n matrix. The transition matrix.
 * @param V A r x r matrix. The observation covariance matrix.
 * @param W A n x n matrix. The state covariance matrix.
 * @param m0 A n x 1 matrix. The mean vector of the distribution
 * of the initial state.
 * @param C0 A n x n matrix. The covariance matrix of the
 * distribution of the initial state.
 * @return The log of the joint density of the GDLM.
 * @throw std::domain_error if a matrix in the Kalman filter is
 * not positive semi-definite.
 * @tparam T_y Type of scalar.
 * @tparam T_F Type of design matrix.
 * @tparam T_G Type of transition matrix.
 * @tparam T_V Type of observation covariance matrix.
 * @tparam T_W Type of state covariance matrix.
 * @tparam T_m0 Type of initial state mean vector.
 * @tparam T_C0 Type of initial state covariance matrix.
 */
template <bool propto, typename T_y, typename T_F, typename T_G, typename T_V,
          typename T_W, typename T_m0, typename T_C0>
typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, Eigen::Dynamic>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  static const char* function = "gaussian_dlm_obs_lpdf";
  typedef
      typename return_type<T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0,
                                                     T_C0>::type>::type T_lp;
  T_lp lp(0.0);

  int r = y.rows();  // number of variables
  int T = y.cols();  // number of observations
  int n = G.rows();  // number of states

  check_finite(function, "y", y);
  check_not_nan(function, "y", y);
  check_size_match(function, "columns of F", F.cols(), "rows of y", y.rows());
  check_size_match(function, "rows of F", F.rows(), "rows of G", G.rows());
  check_finite(function, "F", F);
  check_square(function, "G", G);
  check_finite(function, "G", G);
  check_size_match(function, "rows of V", V.rows(), "rows of y", y.rows());
  // TODO(anyone): incorporate support for infinite V
  check_finite(function, "V", V);
  check_spsd_matrix(function, "V", V);
  check_size_match(function, "rows of W", W.rows(), "rows of G", G.rows());
  // TODO(anyone): incorporate support for infinite W
  check_finite(function, "W", W);
  check_spsd_matrix(function, "W", W);
  check_size_match(function, "size of m0", m0.size(), "rows of G", G.rows());
  check_finite(function, "m0", m0);
  check_size_match(function, "rows of C0", C0.rows(), "rows of G", G.rows());
  check_pos_definite(function, "C0", C0);
  check_finite(function, "C0", C0);

  if (y.cols() == 0 || y.rows() == 0)
    return lp;

  if (include_summand<propto>::value) {
    lp -= 0.5 * LOG_TWO_PI * r * T;
  }

  if (include_summand<propto, T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>::value) {
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> m(n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> C(n, n);

    // TODO(anyone): how to recast matrices
    for (int i = 0; i < m0.size(); i++) {
      m(i) = m0(i);
    }
    for (int i = 0; i < C0.rows(); i++) {
      for (int j = 0; j < C0.cols(); j++) {
        C(i, j) = C0(i, j);
      }
    }

    Eigen::Matrix<typename return_type<T_y>::type, Eigen::Dynamic, 1> yi(r);
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> a(n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> R(n, n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> f(r);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> Q(r, r);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> Q_inv(r, r);
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> e(r);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> A(n, r);

    for (int i = 0; i < y.cols(); i++) {
      yi = y.col(i);
      // // Predict state
      // a_t = G_t m_{t-1}
      a = multiply(G, m);
      // R_t = G_t C_{t-1} G_t' + W_t
      R = add(quad_form_sym(C, transpose(G)), W);
      // // predict observation
      // f_t = F_t' a_t
      f = multiply(transpose(F), a);
      // Q_t = F'_t R_t F_t + V_t
      Q = add(quad_form_sym(R, F), V);
      Q_inv = inverse_spd(Q);
      // // filtered state
      // e_t = y_t - f_t
      e = subtract(yi, f);
      // A_t = R_t F_t Q^{-1}_t
      A = multiply(multiply(R, F), Q_inv);
      // m_t = a_t + A_t e_t
      m = add(a, multiply(A, e));
      // C = R_t - A_t Q_t A_t'
      C = subtract(R, quad_form_sym(Q, transpose(A)));
      lp -= 0.5 * (log_determinant_spd(Q) + trace_quad_form(Q_inv, e));
    }
  }
  return lp;
}

template <typename T_y, typename T_F, typename T_G, typename T_V, typename T_W,
          typename T_m0, typename T_C0>
inline typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, Eigen::Dynamic>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<false>(y, F, G, V, W, m0, C0);
}

/**
 * The log of a Gaussian dynamic linear model (GDLM) with
 * uncorrelated observation disturbances.
 * This distribution is equivalent to, for \f$t = 1:T\f$,
 * \f{eqnarray*}{
 * y_t & \sim N(F' \theta_t, diag(V)) \\
 * \theta_t & \sim N(G \theta_{t-1}, W) \\
 * \theta_0 & \sim N(m_0, C_0)
 * \f}
 *
 * If V is a vector, then the Kalman filter is applied
 * sequentially.
 *
 * @param y A r x T matrix of observations. Rows are variables,
 * columns are observations.
 * @param F A n x r matrix. The design matrix.
 * @param G A n x n matrix. The transition matrix.
 * @param V A size r vector. The diagonal of the observation
 * covariance matrix.
 * @param W A n x n matrix. The state covariance matrix.
 * @param m0 A n x 1 matrix. The mean vector of the distribution
 * of the initial state.
 * @param C0 A n x n matrix. The covariance matrix of the
 * distribution of the initial state.
 * @return The log of the joint density of the GDLM.
 * @throw std::domain_error if a matrix in the Kalman filter is
 * not semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_F Type of design matrix.
 * @tparam T_G Type of transition matrix.
 * @tparam T_V Type of observation variances
 * @tparam T_W Type of state covariance matrix.
 * @tparam T_m0 Type of initial state mean vector.
 * @tparam T_C0 Type of initial state covariance matrix.
 */
template <bool propto, typename T_y, typename T_F, typename T_G, typename T_V,
          typename T_W, typename T_m0, typename T_C0>
typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, 1>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  static const char* function = "gaussian_dlm_obs_lpdf";
  typedef
      typename return_type<T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0,
                                                     T_C0>::type>::type T_lp;
  T_lp lp(0.0);

  using std::log;

  int r = y.rows();  // number of variables
  int T = y.cols();  // number of observations
  int n = G.rows();  // number of states

  check_finite(function, "y", y);
  check_not_nan(function, "y", y);
  check_size_match(function, "columns of F", F.cols(), "rows of y", y.rows());
  check_size_match(function, "rows of F", F.rows(), "rows of G", G.rows());
  check_finite(function, "F", F);
  check_not_nan(function, "F", F);
  check_size_match(function, "rows of G", G.rows(), "columns of G", G.cols());
  check_finite(function, "G", G);
  check_not_nan(function, "G", G);
  check_nonnegative(function, "V", V);
  check_size_match(function, "size of V", V.size(), "rows of y", y.rows());
  // TODO(anyone): support infinite V
  check_finite(function, "V", V);
  check_not_nan(function, "V", V);
  check_spsd_matrix(function, "W", W);
  check_size_match(function, "rows of W", W.rows(), "rows of G", G.rows());
  // TODO(anyone): support infinite W
  check_finite(function, "W", W);
  check_not_nan(function, "W", W);
  check_size_match(function, "size of m0", m0.size(), "rows of G", G.rows());
  check_finite(function, "m0", m0);
  check_not_nan(function, "m0", m0);
  check_pos_definite(function, "C0", C0);
  check_size_match(function, "rows of C0", C0.rows(), "rows of G", G.rows());
  check_finite(function, "C0", C0);
  check_not_nan(function, "C0", C0);

  if (y.cols() == 0 || y.rows() == 0)
    return lp;

  if (include_summand<propto>::value) {
    lp += 0.5 * NEG_LOG_TWO_PI * r * T;
  }

  if (include_summand<propto, T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>::value) {
    T_lp f;
    T_lp Q;
    T_lp Q_inv;
    T_lp e;
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> A(n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> Fj(n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, 1> m(n);
    Eigen::Matrix<T_lp, Eigen::Dynamic, Eigen::Dynamic> C(n, n);

    // TODO(anyone): how to recast matrices
    for (int i = 0; i < m0.size(); i++) {
      m(i) = m0(i);
    }
    for (int i = 0; i < C0.rows(); i++) {
      for (int j = 0; j < C0.cols(); j++) {
        C(i, j) = C0(i, j);
      }
    }

    for (int i = 0; i < y.cols(); i++) {
      // Predict state
      // reuse m and C instead of using a and R
      m = multiply(G, m);
      C = add(quad_form_sym(C, transpose(G)), W);
      for (int j = 0; j < y.rows(); ++j) {
        // predict observation
        T_lp yij(y(j, i));
        // dim Fj = (n, 1)
        for (int k = 0; k < F.rows(); ++k) {
          Fj(k) = F(k, j);
        }
        // f_{t, i} = F_{t, i}' m_{t, i-1}
        f = dot_product(Fj, m);
        Q = trace_quad_form(C, Fj) + V(j);
        Q_inv = 1.0 / Q;
        // filtered observation
        // e_{t, i} = y_{t, i} - f_{t, i}
        e = yij - f;
        // A_{t, i} = C_{t, i-1} F_{t, i} Q_{t, i}^{-1}
        A = multiply(multiply(C, Fj), Q_inv);
        // m_{t, i} = m_{t, i-1} + A_{t, i} e_{t, i}
        m += multiply(A, e);
        // c_{t, i} = C_{t, i-1} - Q_{t, i} A_{t, i} A_{t, i}'
        // tcrossprod throws an error (ambiguous)
        // C = subtract(C, multiply(Q, tcrossprod(A)));
        C -= multiply(Q, multiply(A, transpose(A)));
        C = 0.5 * add(C, transpose(C));
        lp -= 0.5 * (log(Q) + pow(e, 2) * Q_inv);
      }
    }
  }
  return lp;
}

template <typename T_y, typename T_F, typename T_G, typename T_V, typename T_W,
          typename T_m0, typename T_C0>
inline typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_lpdf(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, 1>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<false>(y, F, G, V, W, m0, C0);
}

}  // namespace math
}  // namespace stan
#endif
