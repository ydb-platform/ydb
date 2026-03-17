#ifndef STAN_MATH_PRIM_MAT_FUN_CSR_EXTRACT_U_HPP
#define STAN_MATH_PRIM_MAT_FUN_CSR_EXTRACT_U_HPP

#include <stan/math/prim/scal/meta/error_index.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <Eigen/Sparse>
#include <vector>
#include <numeric>

namespace stan {
namespace math {

/** \addtogroup csr_format
 *  @{
 */

/**
 * Extract the NZE index for each entry from a sparse matrix.
 *
 * @tparam T Type of matrix entries.
 * @param A Sparse matrix.
 * @return Vector of indexes into non-zero entries of A.
 */
template <typename T>
const std::vector<int> csr_extract_u(
    const Eigen::SparseMatrix<T, Eigen::RowMajor>& A) {
  std::vector<int> u(A.outerSize() + 1);  // last entry is garbage.
  for (int nze = 0; nze <= A.outerSize(); ++nze)
    u[nze] = *(A.outerIndexPtr() + nze) + stan::error_index::value;
  return u;
}

/**
 * Extract the NZE index for each entry from a sparse matrix.
 *
 * @tparam T Type of matrix entries.
 * @param A Dense matrix.
 * @return Vector of indexes into non-zero entries of A.
 */
template <typename T, int R, int C>
const std::vector<int> csr_extract_u(const Eigen::Matrix<T, R, C>& A) {
  Eigen::SparseMatrix<T, Eigen::RowMajor> B = A.sparseView();
  std::vector<int> u = csr_extract_u(B);
  return u;
}

/** @} */  // end of csr_format group

}  // namespace math
}  // namespace stan

#endif
