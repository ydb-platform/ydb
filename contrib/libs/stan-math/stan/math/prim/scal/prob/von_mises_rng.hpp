#ifndef STAN_MATH_PRIM_SCAL_PROB_VON_MISES_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_VON_MISES_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a von Mises random variate for the given location and concentration
 * using the specified random number generator.
 *
 * mu and kappa can each be a scalar or a vector. Any non-scalar
 * inputs must be the same length.
 *
 * The algorithm used in von_mises_rng is a modified version of the
 * algorithm in:
 *
 * Efficient Simulation of the von Mises Distribution
 * D. J. Best and N. I. Fisher
 * Journal of the Royal Statistical Society. Series C (Applied Statistics),
 * Vol. 28, No. 2 (1979), pp. 152-157
 *
 * See licenses/stan-license.txt for Stan license.
 *
 * @tparam T_loc Type of location parameter
 * @tparam T_conc Type of concentration parameter
 * @tparam RNG type of random number generator
 * @param mu (Sequence of) location parameter(s)
 * @param kappa (Sequence of) positive concentration parameter(s)
 * @param rng random number generator
 * @return (Sequence of) von Mises random variate(s)
 * @throw std::domain_error if mu is infinite or kappa is nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_loc, typename T_conc, class RNG>
inline typename VectorBuilder<true, double, T_loc, T_conc>::type von_mises_rng(
    const T_loc& mu, const T_conc& kappa, RNG& rng) {
  using boost::random::uniform_real_distribution;
  using boost::variate_generator;
  static const char* function = "von_mises_rng";

  check_finite(function, "mean", mu);
  check_positive_finite(function, "inverse of variance", kappa);
  check_consistent_sizes(function, "Location parameter", mu,
                         "Concentration Parameter", kappa);

  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_conc> kappa_vec(kappa);
  size_t N = max_size(mu, kappa);
  VectorBuilder<true, double, T_loc, T_conc> output(N);

  variate_generator<RNG&, uniform_real_distribution<> > uniform_rng(
      rng, uniform_real_distribution<>(0.0, 1.0));

  for (size_t n = 0; n < N; ++n) {
    double r = 1 + std::pow((1 + 4 * kappa_vec[n] * kappa_vec[n]), 0.5);
    double rho = 0.5 * (r - std::pow(2 * r, 0.5)) / kappa_vec[n];
    double s = 0.5 * (1 + rho * rho) / rho;

    bool done = false;
    double W;
    while (!done) {
      double Z = std::cos(pi() * uniform_rng());
      W = (1 + s * Z) / (s + Z);
      double Y = kappa_vec[n] * (s - W);
      double U2 = uniform_rng();
      done = Y * (2 - Y) - U2 > 0;

      if (!done)
        done = std::log(Y / U2) + 1 - Y >= 0;
    }

    double U3 = uniform_rng() - 0.5;
    double sign = ((U3 >= 0) - (U3 <= 0));

    //  it's really an fmod() with a positivity constraint
    output[n]
        = sign * std::acos(W)
          + std::fmod(std::fmod(mu_vec[n], 2 * pi()) + 2 * stan::math::pi(),
                      2 * pi());
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
