#ifndef STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/gamma_q.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/tgamma.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_gamma.hpp>
#include <boost/random/chi_squared_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <limits>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_dof, typename T_scale>
typename return_type<T_y, T_dof, T_scale>::type scaled_inv_chi_square_lcdf(
    const T_y& y, const T_dof& nu, const T_scale& s) {
  typedef typename stan::partials_return_type<T_y, T_dof, T_scale>::type
      T_partials_return;

  if (size_zero(y, nu, s))
    return 0.0;

  static const char* function = "scaled_inv_chi_square_lcdf";

  using std::exp;

  T_partials_return P(0.0);

  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_positive_finite(function, "Degrees of freedom parameter", nu);
  check_positive_finite(function, "Scale parameter", s);
  check_consistent_sizes(function, "Random variable", y,
                         "Degrees of freedom parameter", nu, "Scale parameter",
                         s);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_dof> nu_vec(nu);
  scalar_seq_view<T_scale> s_vec(s);
  size_t N = max_size(y, nu, s);

  operands_and_partials<T_y, T_dof, T_scale> ops_partials(y, nu, s);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(y); i++) {
    if (value_of(y_vec[i]) == 0)
      return ops_partials.build(negative_infinity());
  }

  using std::exp;
  using std::log;
  using std::pow;

  VectorBuilder<!is_constant_struct<T_dof>::value, T_partials_return, T_dof>
      gamma_vec(stan::length(nu));
  VectorBuilder<!is_constant_struct<T_dof>::value, T_partials_return, T_dof>
      digamma_vec(stan::length(nu));

  if (!is_constant_struct<T_dof>::value) {
    for (size_t i = 0; i < stan::length(nu); i++) {
      const T_partials_return half_nu_dbl = 0.5 * value_of(nu_vec[i]);
      gamma_vec[i] = tgamma(half_nu_dbl);
      digamma_vec[i] = digamma(half_nu_dbl);
    }
  }

  for (size_t n = 0; n < N; n++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(y_vec[n]) == std::numeric_limits<double>::infinity()) {
      continue;
    }

    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return y_inv_dbl = 1.0 / y_dbl;
    const T_partials_return half_nu_dbl = 0.5 * value_of(nu_vec[n]);
    const T_partials_return s_dbl = value_of(s_vec[n]);
    const T_partials_return half_s2_overx_dbl = 0.5 * s_dbl * s_dbl * y_inv_dbl;
    const T_partials_return half_nu_s2_overx_dbl
        = 2.0 * half_nu_dbl * half_s2_overx_dbl;

    const T_partials_return Pn = gamma_q(half_nu_dbl, half_nu_s2_overx_dbl);
    const T_partials_return gamma_p_deriv
        = exp(-half_nu_s2_overx_dbl)
          * pow(half_nu_s2_overx_dbl, half_nu_dbl - 1) / tgamma(half_nu_dbl);

    P += log(Pn);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += half_nu_s2_overx_dbl * y_inv_dbl * gamma_p_deriv / Pn;
    if (!is_constant_struct<T_dof>::value)
      ops_partials.edge2_.partials_[n]
          += (0.5
                  * grad_reg_inc_gamma(half_nu_dbl, half_nu_s2_overx_dbl,
                                       gamma_vec[n], digamma_vec[n])
              - half_s2_overx_dbl * gamma_p_deriv)
             / Pn;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += -2.0 * half_nu_dbl * s_dbl * y_inv_dbl * gamma_p_deriv / Pn;
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
