#ifndef STAN_MATH_PRIM_MAT_FUN_SIMPLEX_CONSTRAIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_SIMPLEX_CONSTRAIN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <stan/math/prim/scal/fun/logit.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the simplex corresponding to the specified free vector.
 * A simplex is a vector containing values greater than or equal
 * to 0 that sum to 1.  A vector with (K-1) unconstrained values
 * will produce a simplex of size K.
 *
 * The transform is based on a centered stick-breaking process.
 *
 * @param y Free vector input of dimensionality K - 1.
 * @return Simplex of dimensionality K.
 * @tparam T Type of scalar.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> simplex_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& y) {
  // cut & paste simplex_constrain(Eigen::Matrix, T) w/o Jacobian
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::log;
  typedef typename index_type<Matrix<T, Dynamic, 1> >::type size_type;

  int Km1 = y.size();
  Matrix<T, Dynamic, 1> x(Km1 + 1);
  T stick_len(1.0);
  for (size_type k = 0; k < Km1; ++k) {
    T z_k(inv_logit(y(k) - log(Km1 - k)));
    x(k) = stick_len * z_k;
    stick_len -= x(k);
  }
  x(Km1) = stick_len;
  return x;
}

/**
 * Return the simplex corresponding to the specified free vector
 * and increment the specified log probability reference with
 * the log absolute Jacobian determinant of the transform.
 *
 * The simplex transform is defined through a centered
 * stick-breaking process.
 *
 * @param y Free vector input of dimensionality K - 1.
 * @param lp Log probability reference to increment.
 * @return Simplex of dimensionality K.
 * @tparam T Type of scalar.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> simplex_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& y, T& lp) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::log;

  typedef typename index_type<Matrix<T, Dynamic, 1> >::type size_type;

  int Km1 = y.size();  // K = Km1 + 1
  Matrix<T, Dynamic, 1> x(Km1 + 1);
  T stick_len(1.0);
  for (size_type k = 0; k < Km1; ++k) {
    double eq_share = -log(Km1 - k);  // = logit(1.0/(Km1 + 1 - k));
    T adj_y_k(y(k) + eq_share);
    T z_k(inv_logit(adj_y_k));
    x(k) = stick_len * z_k;
    lp += log(stick_len);
    lp -= log1p_exp(-adj_y_k);
    lp -= log1p_exp(adj_y_k);
    stick_len -= x(k);  // equivalently *= (1 - z_k);
  }
  x(Km1) = stick_len;  // no Jacobian contrib for last dim
  return x;
}

}  // namespace math

}  // namespace stan

#endif
