#ifndef STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/student_t_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a student-t random variate for the given degrees of freedom,
 * location, and scale using the specified random number generator.
 *
 * nu, mu, and sigma can each be a scalar or a one-dimensional container. Any
 * non-scalar inputs must be the same size.
 *
 * @tparam T_deg Type of degrees of freedom parameter
 * @tparam T_loc Type of location parameter
 * @tparam T_scale Type of scale parameter
 * @tparam RNG type of random number generator
 * @param nu (Sequence of) degrees of freedom parameter(s)
 * @param mu (Sequence of) location parameter(s)
 * @param sigma (Sequence of) scale parameter(s)
 * @param rng random number generator
 * @return Student-t random variate
 * @throw std::domain_error if nu is nonpositive, mu is infinite, or sigma
 * is nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_deg, typename T_loc, typename T_scale, class RNG>
inline typename VectorBuilder<true, double, T_deg, T_loc, T_scale>::type
student_t_rng(const T_deg& nu, const T_loc& mu, const T_scale& sigma,
              RNG& rng) {
  using boost::random::student_t_distribution;
  using boost::variate_generator;
  static const char* function = "student_t_rng";

  check_positive_finite(function, "Degrees of freedom parameter", nu);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Degrees of freedom parameter", nu,
                         "Location parameter", mu, "Scale Parameter", sigma);

  scalar_seq_view<T_deg> nu_vec(nu);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(nu, mu, sigma);
  VectorBuilder<true, double, T_deg, T_loc, T_scale> output(N);

  for (size_t n = 0; n < N; ++n) {
    variate_generator<RNG&, student_t_distribution<> > rng_unit_student_t(
        rng, student_t_distribution<>(nu_vec[n]));
    output[n] = mu_vec[n] + sigma_vec[n] * rng_unit_student_t();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
