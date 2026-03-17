#ifndef STAN_MATH_PRIM_SCAL_PROB_UNIFORM_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_UNIFORM_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_low, typename T_high>
typename return_type<T_y, T_low, T_high>::type uniform_lcdf(
    const T_y& y, const T_low& alpha, const T_high& beta) {
  static const char* function = "uniform_lcdf";
  typedef typename stan::partials_return_type<T_y, T_low, T_high>::type
      T_partials_return;

  using std::log;

  if (size_zero(y, alpha, beta))
    return 0.0;

  T_partials_return cdf_log(0.0);
  check_not_nan(function, "Random variable", y);
  check_finite(function, "Lower bound parameter", alpha);
  check_finite(function, "Upper bound parameter", beta);
  check_greater(function, "Upper bound parameter", beta, alpha);
  check_consistent_sizes(function, "Random variable", y,
                         "Lower bound parameter", alpha,
                         "Upper bound parameter", beta);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_low> alpha_vec(alpha);
  scalar_seq_view<T_high> beta_vec(beta);
  size_t N = max_size(y, alpha, beta);

  operands_and_partials<T_y, T_low, T_high> ops_partials(y, alpha, beta);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    if (y_dbl < value_of(alpha_vec[n]) || y_dbl > value_of(beta_vec[n]))
      return negative_infinity();
    if (y_dbl == value_of(beta_vec[n]))
      return ops_partials.build(0.0);
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
    const T_partials_return beta_dbl = value_of(beta_vec[n]);
    const T_partials_return b_min_a = beta_dbl - alpha_dbl;
    const T_partials_return cdf_log_ = (y_dbl - alpha_dbl) / b_min_a;

    cdf_log += log(cdf_log_);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += 1.0 / b_min_a / cdf_log_;
    if (!is_constant_struct<T_low>::value)
      ops_partials.edge2_.partials_[n]
          += (y_dbl - beta_dbl) / b_min_a / b_min_a / cdf_log_;
    if (!is_constant_struct<T_high>::value)
      ops_partials.edge3_.partials_[n] -= 1.0 / b_min_a;
  }
  return ops_partials.build(cdf_log);
}

}  // namespace math
}  // namespace stan
#endif
