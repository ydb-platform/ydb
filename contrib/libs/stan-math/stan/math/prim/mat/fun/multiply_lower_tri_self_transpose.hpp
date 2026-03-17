#ifndef STAN_MATH_PRIM_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP
#define STAN_MATH_PRIM_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP

#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/scal/fun/square.hpp>

namespace stan {
namespace math {

/**
 * Returns the result of multiplying the lower triangular
 * portion of the input matrix by its own transpose.
 * @param L Matrix to multiply.
 * @return The lower triangular values in L times their own
 * transpose.
 * @throw std::domain_error If the input matrix is not square.
 */
inline matrix_d multiply_lower_tri_self_transpose(const matrix_d& L) {
  int K = L.rows();
  if (K == 0)
    return L;
  if (K == 1) {
    matrix_d result(1, 1);
    result(0) = square(L(0));  // first elt, so don't need double idx
    return result;
  }
  int J = L.cols();
  matrix_d LLt(K, K);
  matrix_d Lt = L.transpose();
  for (int m = 0; m < K; ++m) {
    int k = (J < m + 1) ? J : m + 1;
    LLt(m, m) = Lt.col(m).head(k).squaredNorm();
    for (int n = (m + 1); n < K; ++n)
      LLt(n, m) = LLt(m, n) = Lt.col(m).head(k).dot(Lt.col(n).head(k));
  }
  return LLt;
}

}  // namespace math
}  // namespace stan
#endif
