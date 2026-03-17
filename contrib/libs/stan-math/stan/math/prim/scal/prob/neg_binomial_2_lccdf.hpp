#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LCCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LCCDF_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/prob/neg_binomial_ccdf_log.hpp>

namespace stan {
namespace math {

// Temporary neg_binomial_2_ccdf implementation that
// transforms the input parameters and calls neg_binomial_ccdf
template <typename T_n, typename T_location, typename T_precision>
typename return_type<T_location, T_precision>::type neg_binomial_2_lccdf(
    const T_n& n, const T_location& mu, const T_precision& phi) {
  if (size_zero(n, mu, phi))
    return 0.0;

  static const char* function = "neg_binomial_2_lccdf";
  check_positive_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Precision parameter", phi);
  check_not_nan(function, "Random variable", n);
  check_consistent_sizes(function, "Random variable", n, "Location parameter",
                         mu, "Precision Parameter", phi);

  scalar_seq_view<T_location> mu_vec(mu);
  scalar_seq_view<T_precision> phi_vec(phi);

  size_t size_beta = max_size(mu, phi);

  VectorBuilder<true, typename return_type<T_location, T_precision>::type,
                T_location, T_precision>
      beta_vec(size_beta);
  for (size_t i = 0; i < size_beta; ++i)
    beta_vec[i] = phi_vec[i] / mu_vec[i];

  return neg_binomial_ccdf_log(n, phi, beta_vec.data());
}

}  // namespace math
}  // namespace stan
#endif
