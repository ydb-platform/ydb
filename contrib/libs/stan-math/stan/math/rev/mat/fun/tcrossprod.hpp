#ifndef STAN_MATH_REV_MAT_FUN_TCROSSPROD_HPP
#define STAN_MATH_REV_MAT_FUN_TCROSSPROD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/Eigen_NumTraits.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/dot_product.hpp>
#include <stan/math/rev/mat/fun/dot_self.hpp>
#include <stan/math/rev/mat/fun/columns_dot_self.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the result of post-multiplying a matrix by its
 * own transpose.
 * @param M Matrix to multiply.
 * @return M times its transpose.
 */
inline matrix_v tcrossprod(const matrix_v& M) {
  if (M.rows() == 0)
    return matrix_v(0, 0);
  // if (M.rows() == 1)
  //   return M * M.transpose();

  // WAS JUST THIS
  // matrix_v result(M.rows(), M.rows());
  // return result.setZero().selfadjointView<Eigen::Upper>().rankUpdate(M);

  matrix_v MMt(M.rows(), M.rows());

  vari** vs
      = reinterpret_cast<vari**>(ChainableStack::instance().memalloc_.alloc(
          (M.rows() * M.cols()) * sizeof(vari*)));
  int pos = 0;
  for (int m = 0; m < M.rows(); ++m)
    for (int n = 0; n < M.cols(); ++n)
      vs[pos++] = M(m, n).vi_;
  for (int m = 0; m < M.rows(); ++m)
    MMt(m, m) = var(new internal::dot_self_vari(vs + m * M.cols(), M.cols()));
  for (int m = 0; m < M.rows(); ++m) {
    for (int n = 0; n < m; ++n) {
      MMt(m, n) = var(new internal::dot_product_vari<var, var>(
          vs + m * M.cols(), vs + n * M.cols(), M.cols()));
      MMt(n, m) = MMt(m, n);
    }
  }
  return MMt;
}

}  // namespace math
}  // namespace stan
#endif
