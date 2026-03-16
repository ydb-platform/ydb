#ifndef STAN_MATH_PRIM_MAT_FUN_GP_EXP_QUAD_COV_HPP
#define STAN_MATH_PRIM_MAT_FUN_GP_EXP_QUAD_COV_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/divide.hpp>
#include <stan/math/prim/mat/fun/divide_columns.hpp>
#include <stan/math/prim/mat/fun/squared_distance.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/fun/divide.hpp>
#include <stan/math/prim/scal/fun/exp.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/meta/is_constant.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <cmath>
#include <vector>

namespace stan {
namespace math {
/**
 * Returns a squared exponential kernel.
 *
 * @tparam T_x type for each scalar
 * @tparam T_sigma type of parameter sigma
 * @tparam T_l type of parameter length scale
 *
 * @param x std::vector of scalars that can be used in square distance.
 *    This function assumes each element of x is the same size.
 * @param sigma marginal standard deviation or magnitude
 * @param length_scale length scale
 * @return squared distance
 * @throw std::domain_error if sigma <= 0, l <= 0, or
 *   x is nan or infinite
 */
template <typename T_x, typename T_sigma, typename T_l>
inline
    typename Eigen::Matrix<typename stan::return_type<T_x, T_sigma, T_l>::type,
                           Eigen::Dynamic, Eigen::Dynamic>
    gp_exp_quad_cov(const std::vector<T_x> &x, const T_sigma &sigma,
                    const T_l &length_scale) {
  using std::exp;
  check_positive("gp_exp_quad_cov", "magnitude", sigma);
  check_positive("gp_exp_quad_cov", "length scale", length_scale);

  size_t x_size = x.size();
  Eigen::Matrix<typename stan::return_type<T_x, T_sigma, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x_size, x_size);

  if (x_size == 0)
    return cov;

  for (size_t n = 0; n < x.size(); ++n)
    check_not_nan("gp_exp_quad_cov", "x", x[n]);

  T_sigma sigma_sq = square(sigma);
  T_l neg_half_inv_l_sq = -0.5 / square(length_scale);

  for (size_t j = 0; j < x_size; ++j) {
    cov(j, j) = sigma_sq;
    for (size_t i = j + 1; i < x_size; ++i) {
      cov(i, j)
          = sigma_sq * exp(squared_distance(x[i], x[j]) * neg_half_inv_l_sq);
      cov(j, i) = cov(i, j);
    }
  }
  return cov;
}

/**
 * Returns a squared exponential kernel.
 *
 * @tparam T_x type for each scalar
 * @tparam T_sigma type of parameter sigma
 * @tparam T_l type of each length scale parameter
 *
 * @param x std::vector of Eigen vectors of scalars.
 * @param sigma marginal standard deviation or magnitude
 * @param length_scale std::vector length scale
 * @return squared distance
 * @throw std::domain_error if sigma <= 0, l <= 0, or
 *   x is nan or infinite
 */
template <typename T_x, typename T_sigma, typename T_l>
inline
    typename Eigen::Matrix<typename stan::return_type<T_x, T_sigma, T_l>::type,
                           Eigen::Dynamic, Eigen::Dynamic>
    gp_exp_quad_cov(const std::vector<Eigen::Matrix<T_x, Eigen::Dynamic, 1>> &x,
                    const T_sigma &sigma,
                    const std::vector<T_l> &length_scale) {
  using std::exp;

  check_positive_finite("gp_exp_quad_cov", "magnitude", sigma);
  check_positive_finite("gp_exp_quad_cov", "length scale", length_scale);

  size_t x_size = x.size();
  Eigen::Matrix<typename stan::return_type<T_x, T_sigma, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x_size, x_size);
  if (x_size == 0)
    return cov;

  size_t l_size = length_scale.size();
  check_size_match("gp_exp_quad_cov", "x dimension", x[0].size(),
                   "number of length scales", l_size);

  T_sigma sigma_sq = square(sigma);
  std::vector<
      Eigen::Matrix<typename return_type<T_x, T_l>::type, Eigen::Dynamic, 1>>
      x_new = divide_columns(x, length_scale);

  for (size_t j = 0; j < x_size; ++j) {
    cov(j, j) = sigma_sq;
    for (size_t i = j + 1; i < x_size; ++i) {
      cov(i, j) = sigma_sq * exp(-0.5 * squared_distance(x_new[i], x_new[j]));
      cov(j, i) = cov(i, j);
    }
  }
  return cov;
}

/**
 * Returns a squared exponential kernel.
 *
 * This function is for the cross covariance matrix
 * needed to compute posterior predictive density.
 *
 * @tparam T_x1 type of first std::vector of scalars
 * @tparam T_x2 type of second std::vector of scalars
 *    This function assumes each element of x1 and x2 are the same size.
 * @tparam T_sigma type of sigma
 * @tparam T_l type of of length scale
 *
 * @param x1 std::vector of elements that can be used in square distance
 * @param x2 std::vector of elements that can be used in square distance
 * @param sigma standard deviation
 * @param length_scale length scale
 * @return squared distance
 * @throw std::domain_error if sigma <= 0, l <= 0, or
 *   x is nan or infinite
 */
template <typename T_x1, typename T_x2, typename T_sigma, typename T_l>
inline typename Eigen::Matrix<
    typename stan::return_type<T_x1, T_x2, T_sigma, T_l>::type, Eigen::Dynamic,
    Eigen::Dynamic>
gp_exp_quad_cov(const std::vector<T_x1> &x1, const std::vector<T_x2> &x2,
                const T_sigma &sigma, const T_l &length_scale) {
  using std::exp;

  const char *function_name = "gp_exp_quad_cov";
  check_positive(function_name, "magnitude", sigma);
  check_positive(function_name, "length scale", length_scale);

  size_t x1_size = x1.size();
  size_t x2_size = x2.size();
  Eigen::Matrix<typename stan::return_type<T_x1, T_x2, T_sigma, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x1_size, x2_size);
  if (x1_size == 0 || x2_size == 0)
    return cov;

  for (size_t i = 0; i < x1_size; ++i)
    check_not_nan(function_name, "x1", x1[i]);
  for (size_t i = 0; i < x2_size; ++i)
    check_not_nan(function_name, "x2", x2[i]);

  T_sigma sigma_sq = square(sigma);
  T_l neg_half_inv_l_sq = -0.5 / square(length_scale);

  for (size_t i = 0; i < x1.size(); ++i) {
    for (size_t j = 0; j < x2.size(); ++j) {
      cov(i, j)
          = sigma_sq * exp(squared_distance(x1[i], x2[j]) * neg_half_inv_l_sq);
    }
  }
  return cov;
}

/**
 * Returns a squared exponential kernel.
 *
 * This function is for the cross covariance
 * matrix needed to compute the posterior predictive density.
 *
 * @tparam T_x1 type of first std::vector of elements
 * @tparam T_x2 type of second std::vector of elements
 * @tparam T_s type of sigma
 * @tparam T_l type of length scale
 *
 * @param x1 std::vector of Eigen vectors of scalars.
 * @param x2 std::vector of Eigen vectors of scalars.
 * @param sigma standard deviation
 * @param length_scale std::vector of length scale
 * @return squared distance
 * @throw std::domain_error if sigma <= 0, l <= 0, or
 *   x is nan or infinite
 */
template <typename T_x1, typename T_x2, typename T_s, typename T_l>
inline typename Eigen::Matrix<
    typename stan::return_type<T_x1, T_x2, T_s, T_l>::type, Eigen::Dynamic,
    Eigen::Dynamic>
gp_exp_quad_cov(const std::vector<Eigen::Matrix<T_x1, Eigen::Dynamic, 1>> &x1,
                const std::vector<Eigen::Matrix<T_x2, Eigen::Dynamic, 1>> &x2,
                const T_s &sigma, const std::vector<T_l> &length_scale) {
  using std::exp;
  size_t x1_size = x1.size();
  size_t x2_size = x2.size();
  size_t l_size = length_scale.size();

  Eigen::Matrix<typename stan::return_type<T_x1, T_x2, T_s, T_l>::type,
                Eigen::Dynamic, Eigen::Dynamic>
      cov(x1_size, x2_size);
  if (x1_size == 0 || x2_size == 0)
    return cov;

  const char *function_name = "gp_exp_quad_cov";
  for (size_t i = 0; i < x1_size; ++i)
    check_not_nan(function_name, "x1", x1[i]);
  for (size_t i = 0; i < x2_size; ++i)
    check_not_nan(function_name, "x2", x2[i]);
  check_positive_finite(function_name, "magnitude", sigma);
  check_positive_finite(function_name, "length scale", length_scale);
  check_size_match(function_name, "x dimension", x1[0].size(),
                   "number of length scales", l_size);
  check_size_match(function_name, "x dimension", x2[0].size(),
                   "number of length scales", l_size);

  T_s sigma_sq = square(sigma);

  std::vector<Eigen::Matrix<typename return_type<T_x1, T_l, T_s>::type,
                            Eigen::Dynamic, 1>>
      x1_new = divide_columns(x1, length_scale);
  std::vector<Eigen::Matrix<typename return_type<T_x2, T_l, T_s>::type,
                            Eigen::Dynamic, 1>>
      x2_new = divide_columns(x2, length_scale);

  for (size_t i = 0; i < x1_size; ++i) {
    for (size_t j = 0; j < x2_size; ++j) {
      cov(i, j) = sigma_sq * exp(-0.5 * squared_distance(x1_new[i], x2_new[j]));
    }
  }
  return cov;
}
}  // namespace math
}  // namespace stan
#endif
