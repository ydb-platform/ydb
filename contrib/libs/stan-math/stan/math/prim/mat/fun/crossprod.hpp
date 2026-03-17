#ifndef STAN_MATH_PRIM_MAT_FUN_CROSSPROD_HPP
#define STAN_MATH_PRIM_MAT_FUN_CROSSPROD_HPP

#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/tcrossprod.hpp>

namespace stan {
namespace math {

/**
 * Returns the result of pre-multiplying a matrix by its
 * own transpose.
 * @param M Matrix to multiply.
 * @return Transpose of M times M
 */
inline matrix_d crossprod(const matrix_d& M) {
  return tcrossprod(static_cast<matrix_d>(M.transpose()));
}

}  // namespace math
}  // namespace stan
#endif
