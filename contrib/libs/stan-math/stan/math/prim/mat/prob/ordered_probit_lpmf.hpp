#ifndef STAN_MATH_PRIM_MAT_PROB_ORDERED_PROBIT_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_ORDERED_PROBIT_LPMF_HPP

#include <stan/math/prim/scal/fun/Phi.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/arr/err/check_ordered.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <Eigen/Dense>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the (natural) log probability of the specified integer
 * outcome given the continuous location and specified cutpoints
 * in an ordered probit model.
 *
 * <p>Typically the continous location
 * will be the dot product of a vector of regression coefficients
 * and a vector of predictors for the outcome.
 *
 * @tparam propto True if calculating up to a proportion.
 * @tparam T_loc Location type.
 * @tparam T_cut Cut-point type.
 * @param y Outcome.
 * @param lambda Location.
 * @param c Positive increasing vector of cutpoints.
 * @return Log probability of outcome given location and
 * cutpoints.
 * @throw std::domain_error If the outcome is not between 1 and
 * the number of cutpoints plus 2; if the cutpoint vector is
 * empty; if the cutpoint vector contains a non-positive,
 * non-finite value; or if the cutpoint vector is not sorted in
 * ascending order.
 */
template <bool propto, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    int y, const T_loc& lambda,
    const Eigen::Matrix<T_cut, Eigen::Dynamic, 1>& c) {
  using std::exp;
  using std::log;

  static const char* function = "ordered_probit";

  int K = c.size() + 1;

  check_bounded(function, "Random variable", y, 1, K);
  check_finite(function, "Location parameter", lambda);
  check_greater(function, "Size of cut points parameter", c.size(), 0);
  check_ordered(function, "Cut-points", c);
  check_finite(function, "Cut-points", c);

  if (y == 1) {
    return log1m(Phi(lambda - c[0]));
  } else if (y == K) {
    return log(Phi(lambda - c[K - 2]));
  } else {
    return log(Phi(lambda - c[y - 2]) - Phi(lambda - c[y - 1]));
  }
}

template <typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    int y, const T_loc& lambda,
    const Eigen::Matrix<T_cut, Eigen::Dynamic, 1>& c) {
  return ordered_probit_lpmf<false>(y, lambda, c);
}

/**
 * Returns the (natural) log probability of the specified array
 * of integers given the vector of continuous locations and
 * specified cutpoints in an ordered probit model.
 *
 * <p>Typically the continous location
 * will be the dot product of a vector of regression coefficients
 * and a vector of predictors for the outcome.
 *
 * @tparam propto True if calculating up to a proportion.
 * @tparam T_loc Location type.
 * @tparam T_cut Cut-point type.
 * @param y Array of integers
 * @param lambda Vector of continuous location variables.
 * @param c Positive increasing vector of cutpoints.
 * @return Log probability of outcome given location and
 * cutpoints.
 * @throw std::domain_error If the outcome is not between 1 and
 * the number of cutpoints plus 2; if the cutpoint vector is
 * empty; if the cutpoint vector contains a non-positive,
 * non-finite value; or if the cutpoint vector is not sorted in
 * ascending order.
 * @throw std::invalid_argument If y and lambda are different
 * lengths.
 */
template <bool propto, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    const std::vector<int>& y,
    const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& lambda,
    const Eigen::Matrix<T_cut, Eigen::Dynamic, 1>& c) {
  using std::exp;
  using std::log;

  static const char* function = "ordered_probit";

  int N = lambda.size();
  int K = c.size() + 1;

  check_consistent_sizes(function, "Integers", y, "Locations", lambda);
  check_bounded(function, "Random variable", y, 1, K);
  check_finite(function, "Location parameter", lambda);
  check_ordered(function, "Cut-points", c);
  check_greater(function, "Size of cut points parameter", c.size(), 0);
  check_finite(function, "Cut-points", c);

  typename return_type<T_loc, T_cut>::type logp_n(0.0);

  for (int i = 0; i < N; ++i) {
    if (y[i] == 1) {
      logp_n += log1m(Phi(lambda[i] - c[0]));
    } else if (y[i] == K) {
      logp_n += log(Phi(lambda[i] - c[K - 2]));
    } else {
      logp_n
          += log(Phi(lambda[i] - c[y[i] - 2]) - Phi(lambda[i] - c[y[i] - 1]));
    }
  }
  return logp_n;
}

template <typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    const std::vector<int>& y,
    const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& lambda,
    const Eigen::Matrix<T_cut, Eigen::Dynamic, 1>& c) {
  return ordered_probit_lpmf<false>(y, lambda, c);
}

/**
 * Returns the (natural) log probability of the specified array
 * of integers given the vector of continuous locations and
 * array of specified cutpoints in an ordered probit model.
 *
 * <p>Typically the continous location
 * will be the dot product of a vector of regression coefficients
 * and a vector of predictors for the outcome.
 *
 * @tparam propto True if calculating up to a proportion.
 * @tparam T_y Type of y variable (should be std::vector<int>).
 * @tparam T_loc Location type.
 * @tparam T_cut Cut-point type.
 * @param y Array of integers
 * @param lambda Vector of continuous location variables.
 * @param c array of Positive increasing vectors of cutpoints.
 * @return Log probability of outcome given location and
 * cutpoints.
 * @throw std::domain_error If the outcome is not between 1 and
 * the number of cutpoints plus 2; if the cutpoint vector is
 * empty; if the cutpoint vector contains a non-positive,
 * non-finite value; or if the cutpoint vector is not sorted in
 * ascending order.
 * @throw std::invalid_argument If y and lambda are different
 * lengths, or if y and the array of cutpoints are of different
 * lengths.
 */
template <bool propto, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    const std::vector<int>& y,
    const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& lambda,
    const std::vector<Eigen::Matrix<T_cut, Eigen::Dynamic, 1> >& c) {
  using std::exp;
  using std::log;

  static const char* function = "ordered_probit";

  int N = lambda.size();

  check_consistent_sizes(function, "Integers", y, "Locations", lambda);
  check_consistent_sizes(function, "Integers", y, "Cut-points", c);

  for (int i = 0; i < N; ++i) {
    int K = c[i].size() + 1;
    check_bounded(function, "Random variable", y[i], 1, K);
    check_greater(function, "Size of cut points parameter", c[i].size(), 0);
    check_ordered(function, "Cut-points", c[i]);
  }

  check_finite(function, "Location parameter", lambda);
  check_finite(function, "Cut-points", c);

  typename return_type<T_loc, T_cut>::type logp_n(0.0);

  for (int i = 0; i < N; ++i) {
    int K = c[i].size() + 1;

    if (y[i] == 1) {
      logp_n += log1m(Phi(lambda[i] - c[i][0]));
    } else if (y[i] == K) {
      logp_n += log(Phi(lambda[i] - c[i][K - 2]));
    } else {
      logp_n += log(Phi(lambda[i] - c[i][y[i] - 2])
                    - Phi(lambda[i] - c[i][y[i] - 1]));
    }
  }
  return logp_n;
}

template <typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_probit_lpmf(
    const std::vector<int>& y,
    const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& lambda,
    const std::vector<Eigen::Matrix<T_cut, Eigen::Dynamic, 1> >& c) {
  return ordered_probit_lpmf<false>(y, lambda, c);
}
}  // namespace math
}  // namespace stan
#endif
