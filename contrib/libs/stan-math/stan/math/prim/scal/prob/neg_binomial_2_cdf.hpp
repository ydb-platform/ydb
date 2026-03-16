#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_CDF_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta_dda.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddb.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddz.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <limits>

namespace stan {
namespace math {

template <typename T_n, typename T_location, typename T_precision>
typename return_type<T_location, T_precision>::type neg_binomial_2_cdf(
    const T_n& n, const T_location& mu, const T_precision& phi) {
  static const char* function = "neg_binomial_2_cdf";
  typedef
      typename stan::partials_return_type<T_n, T_location, T_precision>::type
          T_partials_return;

  T_partials_return P(1.0);
  if (size_zero(n, mu, phi))
    return P;

  check_positive_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Precision parameter", phi);
  check_not_nan(function, "Random variable", n);
  check_consistent_sizes(function, "Random variable", n, "Location parameter",
                         mu, "Precision Parameter", phi);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_location> mu_vec(mu);
  scalar_seq_view<T_precision> phi_vec(phi);
  size_t size = max_size(n, mu, phi);

  operands_and_partials<T_location, T_precision> ops_partials(mu, phi);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(n); i++) {
    if (value_of(n_vec[i]) < 0)
      return ops_partials.build(0.0);
  }

  VectorBuilder<!is_constant_struct<T_precision>::value, T_partials_return,
                T_precision>
      digamma_phi_vec(stan::length(phi));

  VectorBuilder<!is_constant_struct<T_precision>::value, T_partials_return,
                T_precision>
      digamma_sum_vec(stan::length(phi));

  if (!is_constant_struct<T_precision>::value) {
    for (size_t i = 0; i < stan::length(phi); i++) {
      const T_partials_return n_dbl = value_of(n_vec[i]);
      const T_partials_return phi_dbl = value_of(phi_vec[i]);

      digamma_phi_vec[i] = digamma(phi_dbl);
      digamma_sum_vec[i] = digamma(n_dbl + phi_dbl + 1);
    }
  }

  for (size_t i = 0; i < size; i++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(n_vec[i]) == std::numeric_limits<int>::max())
      return ops_partials.build(1.0);

    const T_partials_return n_dbl = value_of(n_vec[i]);
    const T_partials_return mu_dbl = value_of(mu_vec[i]);
    const T_partials_return phi_dbl = value_of(phi_vec[i]);

    const T_partials_return p_dbl = phi_dbl / (mu_dbl + phi_dbl);
    const T_partials_return d_dbl
        = 1.0 / ((mu_dbl + phi_dbl) * (mu_dbl + phi_dbl));

    const T_partials_return P_i = inc_beta(phi_dbl, n_dbl + 1.0, p_dbl);

    P *= P_i;

    if (!is_constant_struct<T_location>::value)
      ops_partials.edge1_.partials_[i]
          += -inc_beta_ddz(phi_dbl, n_dbl + 1.0, p_dbl) * phi_dbl * d_dbl / P_i;

    if (!is_constant_struct<T_precision>::value) {
      ops_partials.edge2_.partials_[i]
          += inc_beta_dda(phi_dbl, n_dbl + 1, p_dbl, digamma_phi_vec[i],
                          digamma_sum_vec[i])
                 / P_i
             + inc_beta_ddz(phi_dbl, n_dbl + 1.0, p_dbl) * mu_dbl * d_dbl / P_i;
    }
  }

  if (!is_constant_struct<T_location>::value) {
    for (size_t i = 0; i < stan::length(mu); ++i)
      ops_partials.edge1_.partials_[i] *= P;
  }

  if (!is_constant_struct<T_precision>::value) {
    for (size_t i = 0; i < stan::length(phi); ++i)
      ops_partials.edge2_.partials_[i] *= P;
  }

  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
