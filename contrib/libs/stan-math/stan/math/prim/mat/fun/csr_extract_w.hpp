#ifndef STAN_MATH_PRIM_MAT_FUN_CSR_EXTRACT_W_HPP
#define STAN_MATH_PRIM_MAT_FUN_CSR_EXTRACT_W_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <Eigen/Sparse>

namespace stan {
namespace math {

/** \addtogroup csr_format
 *  @{
 */

/* Extract the non-zero values from a sparse matrix.
 *
 * @tparam T Type of matrix entries.
 * @param[in] A sparse matrix.
 * @return Vector of non-zero entries of A.
 */
template <typename T>
const Eigen::Matrix<T, Eigen::Dynamic, 1> csr_extract_w(
    const Eigen::SparseMatrix<T, Eigen::RowMajor>& A) {
  Eigen::Matrix<T, Eigen::Dynamic, 1> w(A.nonZeros());
  w.setZero();
  for (int nze = 0; nze < A.nonZeros(); ++nze)
    w[nze] = *(A.valuePtr() + nze);
  return w;
}

/* Extract the non-zero values from a dense matrix by converting
 * to sparse and calling the sparse matrix extractor.
 *
 * @tparam T Type of matrix entries.
 * @param[in] A dense matrix.
 * @return Vector of non-zero entries of A.
 */
template <typename T, int R, int C>
const Eigen::Matrix<T, Eigen::Dynamic, 1> csr_extract_w(
    const Eigen::Matrix<T, R, C>& A) {
  Eigen::SparseMatrix<T, Eigen::RowMajor> B = A.sparseView();
  return csr_extract_w(B);
}

/** @} */  // end of csr_format group

}  // namespace math
}  // namespace stan
#endif
