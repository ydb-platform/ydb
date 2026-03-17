#ifndef STAN_MATH_PRIM_SCAL_PROB_STD_NORMAL_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_STD_NORMAL_LPDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * The log of the normal density for the specified scalar(s) given
 * a location of 0 and a scale of 1. y can be either
 * a scalar or a vector.
 *
 * <p>The result log probability is defined to be the sum of the
 * log probabilities for each observation.
 * @tparam T_y Underlying type of scalar in sequence.
 * @param y (Sequence of) scalar(s).
 * @return The log of the product of the densities.
 * @throw std::domain_error if any scalar is nan.
 */
template <bool propto, typename T_y>
typename return_type<T_y>::type std_normal_lpdf(const T_y& y) {
  static const char* function = "std_normal_lpdf";
  typedef typename stan::partials_return_type<T_y>::type T_partials_return;

  if (size_zero(y))
    return 0.0;

  check_not_nan(function, "Random variable", y);

  if (!include_summand<propto, T_y>::value)
    return 0.0;

  operands_and_partials<T_y> ops_partials(y);
  scalar_seq_view<T_y> y_vec(y);
  T_partials_return logp(0.0);
  for (size_t n = 0; n < length(y); n++) {
    const T_partials_return y_val = value_of(y_vec[n]);
    logp += y_val * y_val;
    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] -= y_val;
  }
  logp *= -0.5;
  if (include_summand<propto>::value)
    logp += NEG_LOG_SQRT_TWO_PI * length(y);
  return ops_partials.build(logp);
}

template <typename T_y>
inline typename return_type<T_y>::type std_normal_lpdf(const T_y& y) {
  return std_normal_lpdf<false>(y);
}

}  // namespace math
}  // namespace stan
#endif
