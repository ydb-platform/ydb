#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LCDF_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/prob/beta_cdf_log.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_n, typename T_location, typename T_precision>
typename return_type<T_location, T_precision>::type neg_binomial_2_lcdf(
    const T_n& n, const T_location& mu, const T_precision& phi) {
  using std::log;

  if (size_zero(n, mu, phi))
    return 0.0;

  static const char* function = "neg_binomial_2_lcdf";
  check_positive_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Precision parameter", phi);
  check_not_nan(function, "Random variable", n);
  check_consistent_sizes(function, "Random variable", n, "Location parameter",
                         mu, "Precision Parameter", phi);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_location> mu_vec(mu);
  scalar_seq_view<T_precision> phi_vec(phi);

  size_t size_phi_mu = max_size(mu, phi);
  VectorBuilder<true, typename return_type<T_location, T_precision>::type,
                T_location, T_precision>
      phi_mu(size_phi_mu);
  for (size_t i = 0; i < size_phi_mu; i++)
    phi_mu[i] = phi_vec[i] / (phi_vec[i] + mu_vec[i]);

  size_t size_n = length(n);
  VectorBuilder<true, typename return_type<T_n>::type, T_n> np1(size_n);
  for (size_t i = 0; i < size_n; i++)
    if (n_vec[i] < 0)
      return log(0.0);
    else
      np1[i] = n_vec[i] + 1.0;

  return beta_cdf_log(phi_mu.data(), phi, np1.data());
}

}  // namespace math
}  // namespace stan
#endif
