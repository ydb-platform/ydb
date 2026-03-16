#ifndef STAN_MATH_PRIM_MAT_PROB_WISHART_RNG_HPP
#define STAN_MATH_PRIM_MAT_PROB_WISHART_RNG_HPP

#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/fun/crossprod.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/prob/chi_square_rng.hpp>
#include <stan/math/prim/scal/prob/normal_rng.hpp>

namespace stan {
namespace math {

template <class RNG>
inline Eigen::MatrixXd wishart_rng(double nu, const Eigen::MatrixXd& S,
                                   RNG& rng) {
  static const char* function = "wishart_rng";

  using Eigen::MatrixXd;
  typename index_type<MatrixXd>::type k = S.rows();

  check_square(function, "scale parameter", S);
  check_greater(function, "degrees of freedom > dims - 1", nu, k - 1);

  MatrixXd B = MatrixXd::Zero(k, k);
  for (int j = 0; j < k; ++j) {
    for (int i = 0; i < j; ++i)
      B(i, j) = normal_rng(0, 1, rng);
    B(j, j) = std::sqrt(chi_square_rng(nu - j, rng));
  }
  return crossprod(B * S.llt().matrixU());
}

}  // namespace math
}  // namespace stan
#endif
