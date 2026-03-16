#ifndef STAN_MATH_PRIM_MAT_FUN_FACTOR_U_HPP
#define STAN_MATH_PRIM_MAT_FUN_FACTOR_U_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>

#include <cmath>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

/**
 * This function is intended to make starting values, given a unit
 * upper-triangular matrix U such that U'DU is a correlation matrix
 *
 * @param U Sigma matrix
 * @param CPCs fill this unbounded
 */
template <typename T>
void factor_U(const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& U,
              Eigen::Array<T, Eigen::Dynamic, 1>& CPCs) {
  size_t K = U.rows();
  size_t position = 0;
  size_t pull = K - 1;

  if (K == 2) {
    CPCs(0) = atanh(U(0, 1));
    return;
  }

  Eigen::Array<T, 1, Eigen::Dynamic> temp = U.row(0).tail(pull);

  CPCs.head(pull) = temp;

  Eigen::Array<T, Eigen::Dynamic, 1> acc(K);
  acc(0) = -0.0;
  acc.tail(pull) = 1.0 - temp.square();
  for (size_t i = 1; i < (K - 1); i++) {
    position += pull;
    pull--;
    temp = U.row(i).tail(pull);
    temp /= sqrt(acc.tail(pull) / acc(i));
    CPCs.segment(position, pull) = temp;
    acc.tail(pull) *= 1.0 - temp.square();
  }
  CPCs = 0.5 * ((1.0 + CPCs) / (1.0 - CPCs)).log();  // now unbounded
}

}  // namespace math

}  // namespace stan

#endif
