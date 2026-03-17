#ifndef STAN_MATH_PRIM_MAT_FUN_MATRIX_EXP_PADE_HPP
#define STAN_MATH_PRIM_MAT_FUN_MATRIX_EXP_PADE_HPP

#include <stan/math/prim/mat/fun/MatrixExponential.h>

namespace stan {
namespace math {

/**
 * Computes the matrix exponential, using a Pade
 * approximation, coupled with scaling and
 * squaring.
 *
 * @tparam MatrixType scalar type of the elements
 * in the input matrix.
 * @param[in] arg matrix to exponentiate.
 * @return Matrix exponential of input matrix.
 */
template <typename MatrixType>
MatrixType matrix_exp_pade(const MatrixType& arg) {
  MatrixType U, V;
  int squarings;
  Eigen::matrix_exp_computeUV<MatrixType>::run(arg, U, V, squarings, arg(0, 0));
  // Pade approximant is
  // (U+V) / (-U+V)
  MatrixType numer = U + V;
  MatrixType denom = -U + V;
  MatrixType pade_approximation = denom.partialPivLu().solve(numer);
  for (int i = 0; i < squarings; ++i)
    pade_approximation *= pade_approximation;  // undo scaling by
                                               // repeated squaring
  return pade_approximation;
}
}  // namespace math
}  // namespace stan
#endif
