#ifndef STAN_MATH_PRIM_MAT_FUN_CHOL2INV_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOL2INV_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/dot_self.hpp>
#include <stan/math/prim/mat/fun/dot_product.hpp>
#include <stan/math/prim/mat/fun/mdivide_left_tri_low.hpp>
#include <stan/math/prim/scal/fun/inv_square.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_lower_triangular.hpp>

namespace stan {
namespace math {

/**
 * Returns the inverse of the matrix whose Cholesky factor is L
 * @tparam T The scalar type of the matrix
 * @param L Matrix that is a Cholesky factor.
 * @return The matrix inverse of L * L'
 * @throw std::domain_error If the input matrix is not square or
 *  lower triangular
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> chol2inv(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& L) {
  check_square("chol2inv", "L", L);
  check_lower_triangular("chol2inv", "L", L);
  int K = L.rows();
  typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> matrix_t;
  if (K == 0)
    return L;
  if (K == 1) {
    matrix_t X(1, 1);
    X.coeffRef(0) = inv_square(L.coeff(0));
    return X;
  }
  matrix_t L_inv = mdivide_left_tri_low(L, matrix_t::Identity(K, K).eval());
  matrix_t X(K, K);
  for (int k = 0; k < K; ++k) {
    X.coeffRef(k, k) = dot_self(L_inv.col(k).tail(K - k).eval());
    for (int j = k + 1; j < K; ++j) {
      int Kmj = K - j;
      X.coeffRef(k, j) = X.coeffRef(j, k) = dot_product(
          L_inv.col(k).tail(Kmj).eval(), L_inv.col(j).tail(Kmj).eval());
    }
  }
  return X;
}

}  // namespace math
}  // namespace stan
#endif
