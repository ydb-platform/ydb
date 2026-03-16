#ifndef STAN_MATH_PRIM_SCAL_PROB_RAYLEIGH_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_RAYLEIGH_LCDF_HPP

#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_scale>
typename return_type<T_y, T_scale>::type rayleigh_lcdf(const T_y& y,
                                                       const T_scale& sigma) {
  static const char* function = "rayleigh_lcdf";
  typedef
      typename stan::partials_return_type<T_y, T_scale>::type T_partials_return;

  using std::exp;

  T_partials_return cdf_log(0.0);

  if (size_zero(y, sigma))
    return cdf_log;

  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_not_nan(function, "Scale parameter", sigma);
  check_positive(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y, "Scale parameter",
                         sigma);

  operands_and_partials<T_y, T_scale> ops_partials(y, sigma);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, sigma);

  VectorBuilder<true, T_partials_return, T_scale> inv_sigma(length(sigma));
  for (size_t i = 0; i < length(sigma); i++) {
    inv_sigma[i] = 1.0 / value_of(sigma_vec[i]);
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return y_sqr = y_dbl * y_dbl;
    const T_partials_return inv_sigma_sqr = inv_sigma[n] * inv_sigma[n];
    const T_partials_return exp_val = exp(-0.5 * y_sqr * inv_sigma_sqr);
    const T_partials_return exp_div_1m_exp = exp_val / (1.0 - exp_val);

    if (include_summand<false, T_y, T_scale>::value)
      cdf_log += log1m(exp_val);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += y_dbl * inv_sigma_sqr * exp_div_1m_exp;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge2_.partials_[n]
          -= y_sqr * inv_sigma_sqr * inv_sigma[n] * exp_div_1m_exp;
  }
  return ops_partials.build(cdf_log);
}

}  // namespace math
}  // namespace stan
#endif
