#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta_dda.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddb.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddz.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

template <typename T_n, typename T_shape, typename T_inv_scale>
typename return_type<T_shape, T_inv_scale>::type neg_binomial_cdf(
    const T_n& n, const T_shape& alpha, const T_inv_scale& beta) {
  static const char* function = "neg_binomial_cdf";
  typedef typename stan::partials_return_type<T_n, T_shape, T_inv_scale>::type
      T_partials_return;

  if (size_zero(n, alpha, beta))
    return 1.0;

  T_partials_return P(1.0);

  check_positive_finite(function, "Shape parameter", alpha);
  check_positive_finite(function, "Inverse scale parameter", beta);
  check_consistent_sizes(function, "Failures variable", n, "Shape parameter",
                         alpha, "Inverse scale parameter", beta);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  scalar_seq_view<T_inv_scale> beta_vec(beta);
  size_t size = max_size(n, alpha, beta);

  operands_and_partials<T_shape, T_inv_scale> ops_partials(alpha, beta);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(n); i++) {
    if (value_of(n_vec[i]) < 0)
      return ops_partials.build(0.0);
  }

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return, T_shape>
      digamma_alpha_vec(stan::length(alpha));

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return, T_shape>
      digamma_sum_vec(stan::length(alpha));

  if (!is_constant_struct<T_shape>::value) {
    for (size_t i = 0; i < stan::length(alpha); i++) {
      const T_partials_return n_dbl = value_of(n_vec[i]);
      const T_partials_return alpha_dbl = value_of(alpha_vec[i]);

      digamma_alpha_vec[i] = digamma(alpha_dbl);
      digamma_sum_vec[i] = digamma(n_dbl + alpha_dbl + 1);
    }
  }

  for (size_t i = 0; i < size; i++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(n_vec[i]) == std::numeric_limits<int>::max())
      return ops_partials.build(1.0);

    const T_partials_return n_dbl = value_of(n_vec[i]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[i]);
    const T_partials_return beta_dbl = value_of(beta_vec[i]);

    const T_partials_return p_dbl = beta_dbl / (1.0 + beta_dbl);
    const T_partials_return d_dbl = 1.0 / ((1.0 + beta_dbl) * (1.0 + beta_dbl));

    const T_partials_return P_i = inc_beta(alpha_dbl, n_dbl + 1.0, p_dbl);

    P *= P_i;

    if (!is_constant_struct<T_shape>::value) {
      ops_partials.edge1_.partials_[i]
          += inc_beta_dda(alpha_dbl, n_dbl + 1, p_dbl, digamma_alpha_vec[i],
                          digamma_sum_vec[i])
             / P_i;
    }

    if (!is_constant_struct<T_inv_scale>::value)
      ops_partials.edge2_.partials_[i]
          += inc_beta_ddz(alpha_dbl, n_dbl + 1.0, p_dbl) * d_dbl / P_i;
  }

  if (!is_constant_struct<T_shape>::value) {
    for (size_t i = 0; i < stan::length(alpha); ++i)
      ops_partials.edge1_.partials_[i] *= P;
  }

  if (!is_constant_struct<T_inv_scale>::value) {
    for (size_t i = 0; i < stan::length(beta); ++i)
      ops_partials.edge2_.partials_[i] *= P;
  }

  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
