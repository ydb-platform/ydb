#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_LCCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_LCCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the beta log complementary cumulative distribution function
 * for specified probability, location, and precision parameters:
 * beta_proportion_lccdf(y | mu, kappa) = beta_lccdf(y | mu * kappa, (1 -
 * mu) * kappa).  Any arguments other than scalars must be containers of
 * the same size.  With non-scalar arguments, the return is the sum of
 * the log ccdfs with scalars broadcast as necessary.
 *
 * @tparam T_y type of y
 * @tparam T_loc type of location parameter
 * @tparam T_prec type of precision parameter
 * @param y (Sequence of) scalar(s) between zero and one
 * @param mu (Sequence of) location parameter(s)
 * @param kappa (Sequence of) precision parameter(s)
 * @return log probability or sum of log of probabilities
 * @throw std::domain_error if mu is outside (0, 1)
 * @throw std::domain_error if kappa is nonpositive
 * @throw std::domain_error if 1 - y is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_loc, typename T_prec>
typename return_type<T_y, T_loc, T_prec>::type beta_proportion_lccdf(
    const T_y& y, const T_loc& mu, const T_prec& kappa) {
  typedef typename stan::partials_return_type<T_y, T_loc, T_prec>::type
      T_partials_return;

  static const char* function = "beta_proportion_lccdf";

  if (size_zero(y, mu, kappa))
    return 0.0;

  using boost::math::tools::promote_args;

  T_partials_return ccdf_log(0.0);

  check_positive(function, "Location parameter", mu);
  check_less_or_equal(function, "Location parameter", mu, 1.0);
  check_positive_finite(function, "Precision parameter", kappa);
  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_less_or_equal(function, "Random variable", y, 1.0);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Precision parameter", kappa);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_prec> kappa_vec(kappa);
  size_t N = max_size(y, mu, kappa);

  operands_and_partials<T_y, T_loc, T_prec> ops_partials(y, mu, kappa);

  using std::exp;
  using std::log;
  using std::pow;

  VectorBuilder<contains_nonconstant_struct<T_loc, T_prec>::value,
                T_partials_return, T_loc, T_prec>
      digamma_mukappa(max_size(mu, kappa));
  VectorBuilder<contains_nonconstant_struct<T_loc, T_prec>::value,
                T_partials_return, T_loc, T_prec>
      digamma_kappa_mukappa(max_size(mu, kappa));
  VectorBuilder<contains_nonconstant_struct<T_loc, T_prec>::value,
                T_partials_return, T_prec>
      digamma_kappa(length(kappa));

  if (contains_nonconstant_struct<T_loc, T_prec>::value) {
    for (size_t i = 0; i < max_size(mu, kappa); i++) {
      const T_partials_return mukappa_dbl
          = value_of(mu_vec[i]) * value_of(kappa_vec[i]);
      const T_partials_return kappa_mukappa_dbl
          = value_of(kappa_vec[i]) - mukappa_dbl;

      digamma_mukappa[i] = digamma(mukappa_dbl);
      digamma_kappa_mukappa[i] = digamma(kappa_mukappa_dbl);
    }

    for (size_t i = 0; i < length(kappa); i++) {
      digamma_kappa[i] = digamma(value_of(kappa_vec[i]));
    }
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return kappa_dbl = value_of(kappa_vec[n]);
    const T_partials_return mukappa_dbl = mu_dbl * kappa_dbl;
    const T_partials_return kappa_mukappa_dbl = kappa_dbl - mukappa_dbl;
    const T_partials_return betafunc_dbl
        = exp(lbeta(mukappa_dbl, kappa_mukappa_dbl));
    const T_partials_return Pn
        = 1 - inc_beta(mukappa_dbl, kappa_mukappa_dbl, y_dbl);

    ccdf_log += log(Pn);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] -= pow(1 - y_dbl, kappa_mukappa_dbl - 1)
                                          * pow(y_dbl, mukappa_dbl - 1)
                                          / betafunc_dbl / Pn;

    T_partials_return g1 = 0;
    T_partials_return g2 = 0;

    if (contains_nonconstant_struct<T_loc, T_prec>::value) {
      grad_reg_inc_beta(g1, g2, mukappa_dbl, kappa_mukappa_dbl, y_dbl,
                        digamma_mukappa[n], digamma_kappa_mukappa[n],
                        digamma_kappa[n], betafunc_dbl);
    }
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] -= kappa_dbl * (g1 - g2) / Pn;
    if (!is_constant_struct<T_prec>::value)
      ops_partials.edge3_.partials_[n]
          -= (g1 * mu_dbl + g2 * (1 - mu_dbl)) / Pn;
  }
  return ops_partials.build(ccdf_log);
}

}  // namespace math
}  // namespace stan
#endif
