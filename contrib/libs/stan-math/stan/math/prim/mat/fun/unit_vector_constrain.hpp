#ifndef STAN_MATH_PRIM_MAT_FUN_UNIT_VECTOR_CONSTRAIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_UNIT_VECTOR_CONSTRAIN_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/dot_self.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the unit length vector corresponding to the free vector y.
 *
 * See <a
 * href="https://en.wikipedia.org/wiki/N-sphere#Generating_random_points">the
 * Wikipedia page on generating random points on an N-sphere</a>.
 *
 * @tparam T Scalar type.
 * @param y vector of K unrestricted variables
 * @return Unit length vector of dimension K
 */
template <typename T, int R, int C>
Eigen::Matrix<T, R, C> unit_vector_constrain(const Eigen::Matrix<T, R, C>& y) {
  using std::sqrt;
  check_vector("unit_vector_constrain", "y", y);
  check_nonzero_size("unit_vector_constrain", "y", y);
  T SN = dot_self(y);
  check_positive_finite("unit_vector_constrain", "norm", SN);
  return y / sqrt(SN);
}

/**
 * Return the unit length vector corresponding to the free vector y.
 * See https://en.wikipedia.org/wiki/N-sphere#Generating_random_points
 *
 * @param y vector of K unrestricted variables
 * @return Unit length vector of dimension K
 * @param lp Log probability reference to increment.
 * @tparam T Scalar type.
 */
template <typename T, int R, int C>
Eigen::Matrix<T, R, C> unit_vector_constrain(const Eigen::Matrix<T, R, C>& y,
                                             T& lp) {
  using std::sqrt;
  check_vector("unit_vector_constrain", "y", y);
  check_nonzero_size("unit_vector_constrain", "y", y);
  T SN = dot_self(y);
  check_positive_finite("unit_vector_constrain", "norm", SN);
  lp -= 0.5 * SN;
  return y / sqrt(SN);
}

}  // namespace math
}  // namespace stan
#endif
