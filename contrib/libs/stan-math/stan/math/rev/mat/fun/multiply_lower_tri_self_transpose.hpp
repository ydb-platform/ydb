#ifndef STAN_MATH_REV_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP
#define STAN_MATH_REV_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/dot_product.hpp>
#include <stan/math/rev/mat/fun/dot_self.hpp>
#include <stan/math/rev/mat/fun/columns_dot_self.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

inline matrix_v multiply_lower_tri_self_transpose(const matrix_v& L) {
  // check_square("multiply_lower_tri_self_transpose",
  // L, "L", (double*)0);
  int K = L.rows();
  int J = L.cols();
  matrix_v LLt(K, K);
  if (K == 0)
    return LLt;
  // if (K == 1) {
  //   LLt(0, 0) = L(0, 0) * L(0, 0);
  //   return LLt;
  // }
  int Knz;
  if (K >= J)
    Knz = (K - J) * J + (J * (J + 1)) / 2;
  else  // if (K < J)
    Knz = (K * (K + 1)) / 2;
  vari** vs = reinterpret_cast<vari**>(
      ChainableStack::instance().memalloc_.alloc(Knz * sizeof(vari*)));
  int pos = 0;
  for (int m = 0; m < K; ++m)
    for (int n = 0; n < ((J < (m + 1)) ? J : (m + 1)); ++n) {
      vs[pos++] = L(m, n).vi_;
    }
  for (int m = 0, mpos = 0; m < K; ++m, mpos += (J < m) ? J : m) {
    LLt(m, m) = var(
        new internal::dot_self_vari(vs + mpos, (J < (m + 1)) ? J : (m + 1)));
    for (int n = 0, npos = 0; n < m; ++n, npos += (J < n) ? J : n) {
      LLt(m, n) = LLt(n, m) = var(new internal::dot_product_vari<var, var>(
          vs + mpos, vs + npos, (J < (n + 1)) ? J : (n + 1)));
    }
  }
  return LLt;
}

}  // namespace math
}  // namespace stan
#endif
