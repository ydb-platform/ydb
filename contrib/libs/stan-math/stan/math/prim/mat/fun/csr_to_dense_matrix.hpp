#ifndef STAN_MATH_PRIM_MAT_FUN_CSR_TO_DENSE_MATRIX_HPP
#define STAN_MATH_PRIM_MAT_FUN_CSR_TO_DENSE_MATRIX_HPP

#include <stan/math/prim/mat/err/check_range.hpp>
#include <stan/math/prim/mat/fun/csr_u_to_z.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/dot_product.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <vector>

namespace stan {
namespace math {

/** \addtogroup csr_format
 *  @{
 */
/**
 * Construct a dense Eigen matrix from the CSR format components.
 *
 * @tparam T Type of matrix entries.
 * @param[in] m Number of matrix rows.
 * @param[in] n Number of matrix columns.
 * @param[in] w Values of non-zero matrix entries.
 * @param[in] v Column index for each value in w.
 * @param[in] u Index of where each row starts in w.
 * @return Dense matrix defined by previous arguments.
 * @throw std::domain_error If the arguments do not define a matrix.
 * @throw std::invalid_argument if m/n/w/v/u are not internally
 *   consistent, as defined by the indexing scheme.  Extractors are
 *   defined in Stan which guarantee a consistent set of m/n/w/v/u
 *   for a given sparse matrix.
 * @throw std::out_of_range if any of the indices are out of range.
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> csr_to_dense_matrix(
    int m, int n, const Eigen::Matrix<T, Eigen::Dynamic, 1>& w,
    const std::vector<int>& v, const std::vector<int>& u) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  check_positive("csr_to_dense_matrix", "m", m);
  check_positive("csr_to_dense_matrix", "n", n);
  check_size_match("csr_to_dense_matrix", "m", m, "u", u.size() - 1);
  check_size_match("csr_to_dense_matrix", "w", w.size(), "v", v.size());
  check_size_match("csr_to_dense_matrix", "u/z",
                   u[m - 1] + csr_u_to_z(u, m - 1) - 1, "v", v.size());
  for (int i : v)
    check_range("csr_to_dense_matrix", "v[]", n, i);

  Matrix<T, Dynamic, Dynamic> result(m, n);
  result.setZero();
  for (int row = 0; row < m; ++row) {
    int row_end_in_w = (u[row] - stan::error_index::value) + csr_u_to_z(u, row);
    check_range("csr_to_dense_matrix", "w", w.size(), row_end_in_w);
    for (int nze = u[row] - stan::error_index::value; nze < row_end_in_w;
         ++nze) {
      // row is row index, v[nze] is column index. w[nze] is entry value.
      check_range("csr_to_dense_matrix", "j", n, v[nze]);
      result(row, v[nze] - stan::error_index::value) = w(nze);
    }
  }
  return result;
}
/** @} */  // end of csr_format group

}  // namespace math
}  // namespace stan
#endif
