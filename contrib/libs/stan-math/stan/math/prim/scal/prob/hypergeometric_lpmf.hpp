#ifndef STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_LPMF_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/binomial_coefficient_log.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <boost/math/distributions.hpp>

namespace stan {
namespace math {

// Hypergeometric(n|N, a, b)  [0 <= n <= a;  0 <= N-n <= b;  0 <= N <= a+b]
// n: #white balls drawn;  N: #balls drawn;
// a: #white balls;  b: #black balls
template <bool propto, typename T_n, typename T_N, typename T_a, typename T_b>
double hypergeometric_lpmf(const T_n& n, const T_N& N, const T_a& a,
                           const T_b& b) {
  static const char* function = "hypergeometric_lpmf";

  if (size_zero(n, N, a, b))
    return 0.0;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_N> N_vec(N);
  scalar_seq_view<T_a> a_vec(a);
  scalar_seq_view<T_b> b_vec(b);
  size_t size = max_size(n, N, a, b);

  double logp(0.0);
  check_bounded(function, "Successes variable", n, 0, a);
  check_greater(function, "Draws parameter", N, n);
  for (size_t i = 0; i < size; i++) {
    check_bounded(function, "Draws parameter minus successes variable",
                  N_vec[i] - n_vec[i], 0, b_vec[i]);
    check_bounded(function, "Draws parameter", N_vec[i], 0,
                  a_vec[i] + b_vec[i]);
  }
  check_consistent_sizes(function, "Successes variable", n, "Draws parameter",
                         N, "Successes in population parameter", a,
                         "Failures in population parameter", b);

  if (!include_summand<propto>::value)
    return 0.0;

  for (size_t i = 0; i < size; i++)
    logp += math::binomial_coefficient_log(a_vec[i], n_vec[i])
            + math::binomial_coefficient_log(b_vec[i], N_vec[i] - n_vec[i])
            - math::binomial_coefficient_log(a_vec[i] + b_vec[i], N_vec[i]);
  return logp;
}

template <typename T_n, typename T_N, typename T_a, typename T_b>
inline double hypergeometric_lpmf(const T_n& n, const T_N& N, const T_a& a,
                                  const T_b& b) {
  return hypergeometric_lpmf<false>(n, N, a, b);
}

}  // namespace math
}  // namespace stan
#endif
