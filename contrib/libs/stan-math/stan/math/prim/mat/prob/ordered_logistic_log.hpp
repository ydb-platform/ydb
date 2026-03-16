#ifndef STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_LOG_HPP

#include <stan/math/prim/mat/prob/ordered_logistic_lpmf.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>

namespace stan {
namespace math {

/**
 * Returns the (natural) log probability of the integer/s
 * given the vector of continuous location/s and
 * specified cutpoints in an ordered logistic model.
 *
 * <p>Typically the continous location
 * will be the dot product of a vector of regression coefficients
 * and a vector of predictors for the outcome.
 *
 * @tparam propto True if calculating up to a proportion.
 * @tparam T_y y variable type (int or array of integers).
 * @tparam T_loc Location type (double or vector).
 * @tparam T_cut Cut-point type (vector or array of vectors).
 * @param y Integers
 * @param lambda Continuous location variables.
 * @param c Positive increasing cutpoints.
 * @return Log probability of outcome given location and
 * cutpoints.
 * @throw std::domain_error If the outcome is not between 1 and
 * the number of cutpoints plus 2; if the cutpoint vector is
 * empty; if the cutpoint vector contains a non-positive,
 * non-finite value; or if the cutpoint vector is not sorted in
 * ascending order.
 * @throw std::invalid_argument If array y and vector lambda
 * are different lengths.
 * @throw std::invalid_argument if array y and array of vectors
 * c are different lengths.
 *
 * @deprecated use <code>ordered_logistic_lpmf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_logistic_log(
    const T_y& y, const T_loc& lambda, const T_cut& c) {
  return ordered_logistic_lpmf<propto>(y, lambda, c);
}

/**
 * @deprecated use <code>ordered_logistic_lpmf</code>
 */
template <typename T_y, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_logistic_log(
    const T_y& y, const T_loc& lambda, const T_cut& c) {
  return ordered_logistic_lpmf(y, lambda, c);
}
}  // namespace math
}  // namespace stan
#endif
