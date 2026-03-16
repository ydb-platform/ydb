#ifndef STAN_MATH_PRIM_MAT_PROB_MULTI_NORMAL_RNG_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTI_NORMAL_RNG_HPP

#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/prim/mat/meta/vector_seq_view.hpp>
#include <stan/math/prim/scal/meta/StdVectorBuilder.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a multivariate normal random variate with the given location and
 * covariance using the specified random number generator.
 *
 * mu can be either an Eigen::VectorXd, an Eigen::RowVectorXd, or a std::vector
 * of either of those types.
 *
 * @tparam T_loc Type of location paramater
 * @tparam RNG Type of pseudo-random number generator
 * @param mu (Sequence of) location parameter(s)
 * @param S Covariance matrix
 * @param rng random number generator
 * @throw std::domain_error if S is not positive definite, or
 * std::invalid_argument if the length of (each) mu is not equal to the number
 * of rows and columns in S
 */
template <typename T_loc, class RNG>
inline typename StdVectorBuilder<true, Eigen::VectorXd, T_loc>::type
multi_normal_rng(const T_loc& mu,
                 const Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>& S,
                 RNG& rng) {
  using boost::normal_distribution;
  using boost::variate_generator;

  static const char* function = "multi_normal_rng";

  check_positive(function, "Covariance matrix rows", S.rows());
  check_symmetric(function, "Covariance matrix", S);
  Eigen::LLT<Eigen::MatrixXd> llt_of_S = S.llt();
  check_pos_definite("multi_normal_rng", "covariance matrix argument",
                     llt_of_S);

  vector_seq_view<T_loc> mu_vec(mu);
  size_t size_mu = mu_vec[0].size();

  size_t N = length_mvt(mu);
  int size_mu_old = size_mu;
  for (size_t i = 1; i < N; i++) {
    int size_mu_new = mu_vec[i].size();
    check_size_match(function,
                     "Size of one of the vectors of "
                     "the location variable",
                     size_mu_new,
                     "Size of another vector of the "
                     "location variable",
                     size_mu_old);
    size_mu_old = size_mu_new;
  }

  for (size_t i = 0; i < N; i++) {
    check_finite(function, "Location parameter", mu_vec[i]);
  }

  StdVectorBuilder<true, Eigen::VectorXd, T_loc> output(N);

  variate_generator<RNG&, normal_distribution<> > std_normal_rng(
      rng, normal_distribution<>(0, 1));

  for (size_t n = 0; n < N; ++n) {
    Eigen::VectorXd z(S.cols());
    for (int i = 0; i < S.cols(); i++)
      z(i) = std_normal_rng();

    output[n] = Eigen::VectorXd(mu_vec[n]) + llt_of_S.matrixL() * z;
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
