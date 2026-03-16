#ifndef STAN_MATH_PRIM_MAT_FUN_COV_DOT_PROD_HPP
#define STAN_MATH_PRIM_MAT_FUN_COV_DOT_PROD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/dot_product.hpp>
#include <stan/math/prim/mat/fun/dot_self.hpp>
#include <stan/math/prim/mat/meta/length.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/meta/is_constant.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns a dot product covariance matrix. A member of Stan's Gaussian Process
 * Library.
 *
 * \f$k(x,x') = \sigma^2 + x \cdot x'\f$
 *
 * A dot product covariance matrix is the same covariance matrix
 * as in bayesian regression with \f$N(0,1)\f$ priors on regression coefficients
 * and a \f$N(0,\sigma^2)\f$ prior on the constant function. See Rasmussen and
 * Williams et al 2006, Chapter 4.
 *
 * @tparam T_x type of std::vector of elements
 * @tparam T_sigma type of sigma
 *
 * @param x std::vector of elements that can be used in dot product.
 *    This function assumes each element of x is the same size.
 * @param sigma constant function that can be used in stan::math::square
 * @return dot product covariance matrix that is positive semi-definite
 * @throw std::domain_error if sigma < 0, nan, inf or
 *   x is nan or infinite
 */
template <typename T_x, typename T_sigma>
Eigen::Matrix<typename return_type<T_x, T_sigma>::type, Eigen::Dynamic,
              Eigen::Dynamic>
gp_dot_prod_cov(const std::vector<Eigen::Matrix<T_x, Eigen::Dynamic, 1>> &x,
                const T_sigma &sigma) {
  check_not_nan("gp_dot_prod_cov", "sigma", sigma);
  check_nonnegative("gp_dot_prod_cov", "sigma", sigma);
  check_finite("gp_dot_prod_cov", "sigma", sigma);

  size_t x_size = x.size();
  for (size_t i = 0; i < x_size; ++i) {
    check_not_nan("gp_dot_prod_cov", "x", x[i]);
    check_finite("gp_dot_prod_cov", "x", x[i]);
  }

  Eigen::Matrix<typename stan::return_type<T_x, T_sigma>::type, Eigen::Dynamic,
                Eigen::Dynamic>
      cov(x_size, x_size);
  if (x_size == 0)
    return cov;

  T_sigma sigma_sq = square(sigma);

  for (size_t i = 0; i < (x_size - 1); ++i) {
    cov(i, i) = sigma_sq + dot_self(x[i]);
    for (size_t j = i + 1; j < x_size; ++j) {
      cov(i, j) = sigma_sq + dot_product(x[i], x[j]);
      cov(j, i) = cov(i, j);
    }
  }
  cov(x_size - 1, x_size - 1) = sigma_sq + dot_self(x[x_size - 1]);
  return cov;
}

/**
 * Returns a dot product covariance matrix. A member of Stan's Gaussian
 * Process Library.
 *
 * \f$k(x,x') = \sigma^2 + x \cdot x'\f$
 *
 * A dot product covariance matrix is the same covariance matrix
 * as in bayesian regression with \f$N(0,1)\f$ priors on regression coefficients
 * and a \f$N(0,\sigma^2)\f$ prior on the constant function. See Rasmussen and
 * Williams et al 2006, Chapter 4.
 *
 * @tparam T_x type of std::vector of double
 * @tparam T_sigma type of sigma
 *
 * @param x std::vector of elements that can be used in transpose
 *   and multiply
 *    This function assumes each element of x is the same size.
 * @param sigma constant function that can be used in stan::math::square
 * @return dot product covariance matrix that is positive semi-definite
 * @throw std::domain_error if sigma < 0, nan, inf or
 *   x is nan or infinite
 */
template <typename T_x, typename T_sigma>
Eigen::Matrix<typename return_type<T_x, T_sigma>::type, Eigen::Dynamic,
              Eigen::Dynamic>
gp_dot_prod_cov(const std::vector<T_x> &x, const T_sigma &sigma) {
  check_not_nan("gp_dot_prod_cov", "sigma", sigma);
  check_nonnegative("gp_dot_prod_cov", "sigma", sigma);
  check_finite("gp_dot_prod_cov", "sigma", sigma);

  size_t x_size = x.size();
  check_not_nan("gp_dot_prod_cov", "x", x);
  check_finite("gp_dot_prod_cov", "x", x);

  Eigen::Matrix<typename stan::return_type<T_x, T_sigma>::type, Eigen::Dynamic,
                Eigen::Dynamic>
      cov(x_size, x_size);
  if (x_size == 0)
    return cov;

  T_sigma sigma_sq = square(sigma);

  for (size_t i = 0; i < (x_size - 1); ++i) {
    cov(i, i) = sigma_sq + x[i] * x[i];
    for (size_t j = i + 1; j < x_size; ++j) {
      cov(i, j) = sigma_sq + x[i] * x[j];
      cov(j, i) = cov(i, j);
    }
  }
  cov(x_size - 1, x_size - 1) = sigma_sq + x[x_size - 1] * x[x_size - 1];
  return cov;
}

/**
 * Returns a dot product covariance matrix of differing
 * x's. A member of Stan's Gaussian Process Library.
 *
 * \f$k(x,x') = \sigma^2 + x \cdot x'\f$
 *
 * A dot product covariance matrix is the same covariance matrix
 * as in bayesian regression with \f$N(0,1)\f$ priors on regression coefficients
 * and a \f$N(0,\sigma^2)\f$ prior on the constant function. See Rasmussen and
 * Williams et al 2006, Chapter 4.
 *
 * @tparam T_x1 type of first std::vector of elements
 * @tparam T_x2 type of second std::vector of elements
 * @tparam T_sigma type of sigma
 *
 * @param x1 std::vector of elements that can be used in dot_product
 * @param x2 std::vector of elements that can be used in dot_product
 * @param sigma constant function that can be used in stan::math::square
 * @return dot product covariance matrix that is not always symmetric
 * @throw std::domain_error if sigma < 0, nan or inf
 *   or if x1 or x2 are nan or inf
 */
template <typename T_x1, typename T_x2, typename T_sigma>
Eigen::Matrix<typename return_type<T_x1, T_x2, T_sigma>::type, Eigen::Dynamic,
              Eigen::Dynamic>
gp_dot_prod_cov(const std::vector<Eigen::Matrix<T_x1, Eigen::Dynamic, 1>> &x1,
                const std::vector<Eigen::Matrix<T_x2, Eigen::Dynamic, 1>> &x2,
                const T_sigma &sigma) {
  check_not_nan("gp_dot_prod_cov", "sigma", sigma);
  check_nonnegative("gp_dot_prod_cov", "sigma", sigma);
  check_finite("gp_dot_prod_cov", "sigma", sigma);

  size_t x1_size = x1.size();
  size_t x2_size = x2.size();
  for (size_t i = 0; i < x1_size; ++i) {
    check_not_nan("gp_dot_prod_cov", "x1", x1[i]);
    check_finite("gp_dot_prod_cov", "x1", x1[i]);
  }
  for (size_t i = 0; i < x2_size; ++i) {
    check_not_nan("gp_dot_prod_cov", "x2", x2[i]);
    check_finite("gp_dot_prod_cov", "x2", x2[i]);
  }
  Eigen::Matrix<typename return_type<T_x1, T_x2, T_sigma>::type, Eigen::Dynamic,
                Eigen::Dynamic>
      cov(x1_size, x2_size);

  if (x1_size == 0 || x2_size == 0)
    return cov;

  T_sigma sigma_sq = square(sigma);

  for (size_t i = 0; i < x1_size; ++i) {
    for (size_t j = 0; j < x2_size; ++j) {
      cov(i, j) = sigma_sq + dot_product(x1[i], x2[j]);
    }
  }
  return cov;
}

/**
 * Returns a dot product covariance matrix of
 * differing x's. A member of Stan's Gaussian Process Library.
 *
 * \f$k(x,x') = \sigma^2 + x \cdot x'\f$
 *
 * A dot product covariance matrix is the same covariance matrix
 * as in bayesian regression with \f$N(0,1)\f$ priors on regression coefficients
 * and a \f$N(0,\sigma^2)\f$ prior on the constant function. See Rasmussen and
 * Williams et al 2006, Chapter 4.
 *
 * @tparam T_x1 type of first std::vector of double
 * @tparam T_x2 type of second std::vector of double
 * @tparam T_sigma type of sigma
 *
 * @param x1 std::vector of elements that can be used in dot_product
 * @param x2 std::vector of elements that can be used in dot_product
 * @param sigma is the constant function that can be used in stan::math::square
 * @return dot product covariance matrix that is not always symmetric
 * @throw std::domain_error if sigma < 0, nan or inf
 *   or if x1 or x2 are nan or inf
 */
template <typename T_x1, typename T_x2, typename T_sigma>
Eigen::Matrix<typename return_type<T_x1, T_x2, T_sigma>::type, Eigen::Dynamic,
              Eigen::Dynamic>
gp_dot_prod_cov(const std::vector<T_x1> &x1, const std::vector<T_x2> &x2,
                const T_sigma &sigma) {
  check_not_nan("gp_dot_prod_cov", "sigma", sigma);
  check_nonnegative("gp_dot_prod_cov", "sigma", sigma);
  check_finite("gp_dot_prod_cov", "sigma", sigma);

  size_t x1_size = x1.size();
  size_t x2_size = x2.size();
  check_not_nan("gp_dot_prod_cov", "x1", x1);
  check_finite("gp_dot_prod_cov", "x1", x1);
  check_not_nan("gp_dot_prod_cov", "x2", x2);
  check_finite("gp_dot_prod_cov", "x2", x2);

  Eigen::Matrix<typename stan::return_type<T_x1, T_x2, T_sigma>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x1_size, x2_size);

  if (x1_size == 0 || x2_size == 0)
    return cov;

  T_sigma sigma_sq = square(sigma);

  for (size_t i = 0; i < x1_size; ++i) {
    for (size_t j = 0; j < x2_size; ++j) {
      cov(i, j) = sigma_sq + x1[i] * x2[j];
    }
  }
  return cov;
}
}  // namespace math
}  // namespace stan
#endif
