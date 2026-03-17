#ifndef STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_LPMF_HPP

#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/prim/mat/fun/size.hpp>
#include <stan/math/prim/mat/meta/vector_seq_view.hpp>
#include <stan/math/prim/mat/meta/length_mvt.hpp>
#include <stan/math/prim/mat/err/check_ordered.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit_diff.hpp>
#include <stan/math/prim/scal/fun/is_integer.hpp>
#include <stan/math/prim/scal/err/domain_error_vec.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the (natural) log probability of the specified array
 * of integers given the vector of continuous locations and
 * specified cutpoints in an ordered logistic model.
 *
 * <p>Typically the continous location
 * will be the dot product of a vector of regression coefficients
 * and a vector of predictors for the outcome
 *
  \f[
    \frac{\partial }{\partial \lambda} =
    \begin{cases}\\
    -\mathrm{logit}^{-1}(\lambda - c_1) & \mbox{if } k = 1,\\
    -(((1-e^{c_{k-1}-c_{k-2}})^{-1} - \mathrm{logit}^{-1}(c_{k-2}-\lambda)) +
    ((1-e^{c_{k-2}-c_{k-1}})^{-1} - \mathrm{logit}^{-1}(c_{k-1}-\lambda)))
    & \mathrm{if } 1 < k < K, \mathrm{and}\\
    \mathrm{logit}^{-1}(c_{K-2}-\lambda) & \mathrm{if } k = K.
    \end{cases}
  \f]

  \f[
    \frac{\partial }{\partial \lambda} =
    \begin{cases}
    -\mathrm{logit}^{-1}(\lambda - c_1) & \text{if } k = 1,\\
    -(((1-e^{c_{k-1}-c_{k-2}})^{-1} - \mathrm{logit}^{-1}(c_{k-2}-\lambda)) +
    ((1-e^{c_{k-2}-c_{k-1}})^{-1} - \mathrm{logit}^{-1}(c_{k-1}-\lambda)))
    & \text{if } 1 < k < K, \text{ and}\\
    \mathrm{logit}^{-1}(c_{K-2}-\lambda) & \text{if } k = K.
    \end{cases}
  \f]
 *
 * @tparam propto True if calculating up to a proportion.
 * @tparam T_y Y variable type (integer or array of integers).
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
template <bool propto, typename T_y, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_logistic_lpmf(
    const T_y& y, const T_loc& lambda, const T_cut& c) {
  static const char* function = "ordered_logistic";

  typedef
      typename stan::partials_return_type<T_loc, T_cut>::type T_partials_return;
  typedef typename Eigen::Matrix<T_partials_return, -1, 1> T_partials_vec;

  scalar_seq_view<T_loc> lam_vec(lambda);
  scalar_seq_view<T_y> y_vec(y);
  vector_seq_view<T_cut> c_vec(c);

  int K = c_vec[0].size() + 1;
  int N = length(lambda);
  int C_l = length_mvt(c);

  check_consistent_sizes(function, "Integers", y, "Locations", lambda);
  if (C_l > 1)
    check_size_match(function, "Length of location variables ", N,
                     "Number of cutpoint vectors ", C_l);

  int size_c_old = c_vec[0].size();
  for (int i = 1; i < C_l; i++) {
    int size_c_new = c_vec[i].size();

    check_size_match(function, "Size of one of the vectors of cutpoints ",
                     size_c_new, "Size of another vector of the cutpoints ",
                     size_c_old);
  }

  for (int n = 0; n < N; n++) {
    check_bounded(function, "Random variable", y_vec[n], 1, K);
    check_finite(function, "Location parameter", lam_vec[n]);
  }

  for (int i = 0; i < C_l; i++) {
    check_ordered(function, "Cut-points", c_vec[i]);
    check_greater(function, "Size of cut points parameter", c_vec[i].size(), 0);
    check_finite(function, "Final cut-point", c_vec[i](c_vec[i].size() - 1));
    check_finite(function, "First cut-point", c_vec[i](0));
  }

  operands_and_partials<T_loc, T_cut> ops_partials(lambda, c);

  T_partials_return logp(0.0);
  T_partials_vec c_dbl = value_of(c_vec[0]).template cast<T_partials_return>();

  for (int n = 0; n < N; ++n) {
    if (C_l > 1)
      c_dbl = value_of(c_vec[n]).template cast<T_partials_return>();
    T_partials_return lam_dbl = value_of(lam_vec[n]);

    if (y_vec[n] == 1) {
      logp -= log1p_exp(lam_dbl - c_dbl[0]);
      T_partials_return d = inv_logit(lam_dbl - c_dbl[0]);

      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge1_.partials_[n] -= d;

      if (!is_constant_struct<T_cut>::value)
        ops_partials.edge2_.partials_vec_[n](0) = d;

    } else if (y_vec[n] == K) {
      logp -= log1p_exp(c_dbl[K - 2] - lam_dbl);
      T_partials_return d = inv_logit(c_dbl[K - 2] - lam_dbl);

      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge1_.partials_[n] = d;

      if (!is_constant_struct<T_cut>::value)
        ops_partials.edge2_.partials_vec_[n](K - 2) -= d;

    } else {
      T_partials_return d1
          = inv(1 - exp(c_dbl[y_vec[n] - 1] - c_dbl[y_vec[n] - 2]))
            - inv_logit(c_dbl[y_vec[n] - 2] - lam_dbl);
      T_partials_return d2
          = inv(1 - exp(c_dbl[y_vec[n] - 2] - c_dbl[y_vec[n] - 1]))
            - inv_logit(c_dbl[y_vec[n] - 1] - lam_dbl);
      logp += log_inv_logit_diff(lam_dbl - c_dbl[y_vec[n] - 2],
                                 lam_dbl - c_dbl[y_vec[n] - 1]);

      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge1_.partials_[n] -= d1 + d2;

      if (!is_constant_struct<T_cut>::value) {
        ops_partials.edge2_.partials_vec_[n](y_vec[n] - 2) += d1;
        ops_partials.edge2_.partials_vec_[n](y_vec[n] - 1) += d2;
      }
    }
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_loc, typename T_cut>
typename return_type<T_loc, T_cut>::type ordered_logistic_lpmf(
    const T_y& y, const T_loc& lambda, const T_cut& c) {
  return ordered_logistic_lpmf<false>(y, lambda, c);
}

}  // namespace math
}  // namespace stan
#endif
