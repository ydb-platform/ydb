#ifndef STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_LPMF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/gamma_q.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/poisson_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

// PoissonLog(n|alpha)  [n >= 0]   = Poisson(n|exp(alpha))
template <bool propto, typename T_n, typename T_log_rate>
typename return_type<T_log_rate>::type poisson_log_lpmf(
    const T_n& n, const T_log_rate& alpha) {
  typedef typename stan::partials_return_type<T_n, T_log_rate>::type
      T_partials_return;

  static const char* function = "poisson_log_lpmf";

  using std::exp;

  if (size_zero(n, alpha))
    return 0.0;

  T_partials_return logp(0.0);

  check_nonnegative(function, "Random variable", n);
  check_not_nan(function, "Log rate parameter", alpha);
  check_consistent_sizes(function, "Random variable", n, "Log rate parameter",
                         alpha);

  if (!include_summand<propto, T_log_rate>::value)
    return 0.0;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_log_rate> alpha_vec(alpha);
  size_t size = max_size(n, alpha);

  // FIXME: first loop size of alpha_vec, second loop if-ed for size==1
  for (size_t i = 0; i < size; i++)
    if (std::numeric_limits<double>::infinity() == alpha_vec[i])
      return LOG_ZERO;
  for (size_t i = 0; i < size; i++)
    if (-std::numeric_limits<double>::infinity() == alpha_vec[i]
        && n_vec[i] != 0)
      return LOG_ZERO;

  operands_and_partials<T_log_rate> ops_partials(alpha);

  // FIXME: cache value_of for alpha_vec?  faster if only one?
  VectorBuilder<include_summand<propto, T_log_rate>::value, T_partials_return,
                T_log_rate>
      exp_alpha(length(alpha));
  for (size_t i = 0; i < length(alpha); i++)
    if (include_summand<propto, T_log_rate>::value)
      exp_alpha[i] = exp(value_of(alpha_vec[i]));

  for (size_t i = 0; i < size; i++) {
    if (!(alpha_vec[i] == -std::numeric_limits<double>::infinity()
          && n_vec[i] == 0)) {
      if (include_summand<propto>::value)
        logp -= lgamma(n_vec[i] + 1.0);
      if (include_summand<propto, T_log_rate>::value)
        logp += n_vec[i] * value_of(alpha_vec[i]) - exp_alpha[i];
    }

    if (!is_constant_struct<T_log_rate>::value)
      ops_partials.edge1_.partials_[i] += n_vec[i] - exp_alpha[i];
  }
  return ops_partials.build(logp);
}

template <typename T_n, typename T_log_rate>
inline typename return_type<T_log_rate>::type poisson_log_lpmf(
    const T_n& n, const T_log_rate& alpha) {
  return poisson_log_lpmf<false>(n, alpha);
}

}  // namespace math
}  // namespace stan
#endif
