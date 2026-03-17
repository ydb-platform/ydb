#ifndef STAN_MATH_PRIM_MAT_FUN_GP_MATERN32_COV_HPP
#define STAN_MATH_PRIM_MAT_FUN_GP_MATERN32_COV_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/divide_columns.hpp>
#include <stan/math/prim/mat/fun/distance.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/fun/divide.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/squared_distance.hpp>
#include <stan/math/prim/scal/fun/distance.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/meta/size_of.hpp>
#include <cmath>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns a Matern 3/2 covariance matrix
 *
 * \f[ k(x, x') = \sigma^2(1 +
 *  \frac{\sqrt{3}d(x, x')}{l})exp(-\frac{\sqrt{3}d(x, x')}{l})
 * \f]
 *
 * where \f$ d(x, x') \f$ is the Euclidean distance.
 *
 * @tparam T_x type for each scalar
 * @tparam T_s type of parameter of sigma
 * @tparam T_l type of parameter length scale
 *
 * @param x std::vector of scalars that can be used in squared distance
 * @param length_scale length scale
 * @param sigma marginal standard deviation or magnitude
 * @throw std::domain error if sigma <= 0, l <= 0, or x is nan or inf
 */
template <typename T_x, typename T_s, typename T_l>
inline typename Eigen::Matrix<typename return_type<T_x, T_s, T_l>::type,
                              Eigen::Dynamic, Eigen::Dynamic>
gp_matern32_cov(const std::vector<T_x> &x, const T_s &sigma,
                const T_l &length_scale) {
  using std::exp;
  using std::pow;

  size_t x_size = size_of(x);
  Eigen::Matrix<typename return_type<T_x, T_s, T_l>::type, Eigen::Dynamic,
                Eigen::Dynamic>
      cov(x_size, x_size);

  if (x_size == 0)
    return cov;

  const char *function = "gp_matern32_cov";
  size_t x_obs_size = size_of(x[0]);
  for (size_t i = 0; i < x_size; ++i)
    check_size_match(function, "x row", x_obs_size, "x's other row",
                     size_of(x[i]));

  for (size_t n = 0; n < x_size; ++n)
    check_not_nan(function, "x", x[n]);

  check_positive_finite(function, "magnitude", sigma);
  check_positive_finite(function, "length scale", length_scale);

  T_s sigma_sq = square(sigma);
  T_l root_3_inv_l = sqrt(3.0) / length_scale;
  T_l neg_root_3_inv_l = -1.0 * sqrt(3.0) / length_scale;

  for (size_t i = 0; i < x_size; ++i) {
    cov(i, i) = sigma_sq;
    for (size_t j = i + 1; j < x_size; ++j) {
      typename return_type<T_x>::type dist = distance(x[i], x[j]);
      cov(i, j) = sigma_sq * (1.0 + root_3_inv_l * dist)
                  * exp(neg_root_3_inv_l * dist);
      cov(j, i) = cov(i, j);
    }
  }
  return cov;
}

/**
 * Returns a Matern 3/2 covariance matrix
 *
 * \f[ k(x, x') = \sigma^2(1 + \sqrt{3}
 *   \sqrt{\sum_{k=1}^{K}\frac{d(x, x')^2}{l_k^2}})
 *   exp(-\sqrt{3}\sqrt{\sum_{k=1}^{K}\frac{d(x, x')^2}{l_k^2}}) \f]
 *
 * where \f$d(x, x')\f$ is the Euclidean distance.
 *
 * @tparam T_x type for each scalar
 * @tparam T_s type of element of parameter sigma
 * @tparam T_l type of each length scale parameter
 *
 * @param x std::vector of Eigen vectors of scalars.
 * @param length_scale std::vector of length scales
 * @param sigma standard deviation that can be used in stan::math::square
 * @throw std::domain error if sigma <= 0, l <= 0, or x is nan or inf
 */
template <typename T_x, typename T_s, typename T_l>
inline typename Eigen::Matrix<typename return_type<T_x, T_s, T_l>::type,
                              Eigen::Dynamic, Eigen::Dynamic>
gp_matern32_cov(const std::vector<Eigen::Matrix<T_x, -1, 1>> &x,
                const T_s &sigma, const std::vector<T_l> &length_scale) {
  using std::exp;

  size_t x_size = size_of(x);
  Eigen::Matrix<typename return_type<T_x, T_s, T_l>::type, Eigen::Dynamic,
                Eigen::Dynamic>
      cov(x_size, x_size);

  if (x_size == 0)
    return cov;
  const char *function = "gp_matern32_cov";
  size_t l_size = length_scale.size();
  for (size_t n = 0; n < x_size; ++n)
    check_not_nan(function, "x", x[n]);

  check_positive_finite(function, "magnitude", sigma);
  check_positive_finite(function, "length scale", length_scale);

  for (size_t n = 0; n < x_size; ++n)
    check_not_nan(function, "length scale", length_scale[n]);

  check_size_match(function, "x dimension", size_of(x[0]),
                   "number of length scales", l_size);

  T_s sigma_sq = square(sigma);
  double root_3 = sqrt(3.0);
  double neg_root_3 = -1.0 * sqrt(3.0);

  std::vector<Eigen::Matrix<typename return_type<T_x, T_l>::type, -1, 1>> x_new
      = divide_columns(x, length_scale);

  for (size_t i = 0; i < x_size; ++i) {
    for (size_t j = i; j < x_size; ++j) {
      typename return_type<T_x, T_l>::type dist = distance(x_new[i], x_new[j]);
      cov(i, j) = sigma_sq * (1.0 + root_3 * dist) * exp(neg_root_3 * dist);
      cov(j, i) = cov(i, j);
    }
  }
  return cov;
}

/**
 * Returns a Matern 3/2 cross covariance matrix
 *
 * \f[ k(x, x') = \sigma^2(1 +
 *  \frac{\sqrt{3}d(x, x')}{l})exp(-\sqrt{3}\frac{d(x, x')}{l})
 * \f]
 *
 * where \f$d(x, x')\f$ is the Euclidean distance.
 *
 * This function is for the cross covariance matrix neededed
 * to compute the posterior predictive density.
 *
 * @tparam T_x1 first type of scalars contained in vector x1
 * @tparam T_x2 second type of scalars contained in vector x2
 * @tparam T_s type of parameter sigma, marginal standard deviation
 * @tparam T_l type of parameter length scale
 *
 * @param x1 std::vector of scalars that can be used in squared_distance
 * @param x2 std::vector of scalars that can be used in squared_distance
 * @param length_scale length scale
 * @param sigma standard deviation that can be used in stan::math::square
 * @throw std::domain error if sigma <= 0, l <= 0, or x1, x2 are nan or inf
 *
 */
template <typename T_x1, typename T_x2, typename T_s, typename T_l>
inline typename Eigen::Matrix<typename return_type<T_x1, T_x2, T_s, T_l>::type,
                              Eigen::Dynamic, Eigen::Dynamic>
gp_matern32_cov(const std::vector<T_x1> &x1, const std::vector<T_x2> &x2,
                const T_s &sigma, const T_l &length_scale) {
  using std::exp;

  size_t x1_size = size_of(x1);
  size_t x2_size = size_of(x2);
  Eigen::Matrix<typename return_type<T_x1, T_x2, T_s, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x1_size, x2_size);

  if (x1_size == 0 || x2_size == 0)
    return cov;

  const char *function = "gp_matern32_cov";
  size_t x1_obs_size = size_of(x1[0]);
  for (size_t i = 0; i < x1_size; ++i)
    check_size_match(function, "x1's row", x1_obs_size, "x1's other row",
                     size_of(x1[i]));
  for (size_t i = 0; i < x2_size; ++i)
    check_size_match(function, "x1's row", x1_obs_size, "x2's other row",
                     size_of(x2[i]));

  for (size_t n = 0; n < x1_size; ++n)
    check_not_nan(function, "x1", x1[n]);
  for (size_t n = 0; n < x2_size; ++n)
    check_not_nan(function, "x2", x2[n]);

  check_positive_finite(function, "magnitude", sigma);
  check_positive_finite(function, "length scale", length_scale);

  T_s sigma_sq = square(sigma);
  T_l root_3_inv_l_sq = sqrt(3.0) / length_scale;
  T_l neg_root_3_inv_l_sq = -1.0 * sqrt(3.0) / length_scale;

  for (size_t i = 0; i < x1_size; ++i) {
    for (size_t j = 0; j < x2_size; ++j) {
      typename return_type<T_x1, T_x2>::type dist = distance(x1[i], x2[j]);
      cov(i, j) = sigma_sq * (1.0 + root_3_inv_l_sq * dist)
                  * exp(neg_root_3_inv_l_sq * dist);
    }
  }
  return cov;
}

/**
 * Returns a Matern 3/2 cross covariance matrix
 *
 * \f[ k(x, x') = \sigma^2(1 + \sqrt{3}
 *   \sqrt{\sum_{k=1}^{K}\frac{d(x, x')^2}{l_k^2}})
 *   exp(-\sqrt{3}\sqrt{\sum_{k=1}^{K}\frac{d(x, x')^2}{l_k^2}}) \f]
 *
 * where \f$d(x, x')\f$ is the Euclidean distance
 *
 * This function is for the cross covariance matrix neededed
 * to compute the posterior predictive density.
 *
 * @tparam T_x1 first type of std::vector of scalars
 * @tparam T_x2 second type of std::vector of scalars
 * @tparam T_s type of parameter sigma, marginal standard deviation
 * @tparam T_l type of parameter length scale
 *
 * @param x1 std::vector of Eigen vectors of scalars
 * @param x2 std::vector of Eigen vectors of scalars
 * @param length_scale parameter length scale
 * @param sigma standard deviation that can be used in stan::math::square
 * @throw std::domain error if sigma <= 0, l <= 0, or x1, x2 are nan or inf
 *
 */
template <typename T_x1, typename T_x2, typename T_s, typename T_l>
inline typename Eigen::Matrix<typename return_type<T_x1, T_x2, T_s, T_l>::type,
                              Eigen::Dynamic, Eigen::Dynamic>
gp_matern32_cov(const std::vector<Eigen::Matrix<T_x1, -1, 1>> &x1,
                const std::vector<Eigen::Matrix<T_x2, -1, 1>> &x2,
                const T_s &sigma, const std::vector<T_l> &length_scale) {
  using std::exp;

  size_t x1_size = size_of(x1);
  size_t x2_size = size_of(x2);
  Eigen::Matrix<typename return_type<T_x1, T_x2, T_s, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x1_size, x2_size);

  if (x1_size == 0 || x2_size == 0)
    return cov;

  const char *function = "gp_matern_32_cov";
  for (size_t n = 0; n < x1_size; ++n)
    check_not_nan(function, "x1", x1[n]);
  for (size_t n = 0; n < x2_size; ++n)
    check_not_nan(function, "x2", x2[n]);

  check_positive_finite(function, "magnitude", sigma);
  check_positive_finite(function, "length scale", length_scale);

  size_t l_size = length_scale.size();
  for (size_t n = 0; n < l_size; ++n)
    check_not_nan(function, "length scale", length_scale[n]);

  for (size_t i = 0; i < x1_size; ++i)
    check_size_match(function, "x1's row", size_of(x1[i]),
                     "number of length scales", l_size);
  for (size_t i = 0; i < x2_size; ++i)
    check_size_match(function, "x2's row", size_of(x2[i]),
                     "number of length scales", l_size);

  T_s sigma_sq = square(sigma);
  double root_3 = sqrt(3.0);
  double neg_root_3 = -1.0 * sqrt(3.0);

  std::vector<Eigen::Matrix<typename return_type<T_x1, T_l>::type, -1, 1>>
      x1_new = divide_columns(x1, length_scale);
  std::vector<Eigen::Matrix<typename return_type<T_x2, T_l>::type, -1, 1>>
      x2_new = divide_columns(x2, length_scale);

  for (size_t i = 0; i < x1_size; ++i) {
    for (size_t j = 0; j < x2_size; ++j) {
      typename return_type<T_x1, T_x2, T_l>::type dist
          = distance(x1_new[i], x2_new[j]);
      cov(i, j) = sigma_sq * (1.0 + root_3 * dist) * exp(neg_root_3 * dist);
    }
  }
  return cov;
}
}  // namespace math
}  // namespace stan
#endif
