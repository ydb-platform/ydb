#ifndef STAN_MATH_PRIM_MAT_FUN_QUAD_FORM_HPP
#define STAN_MATH_PRIM_MAT_FUN_QUAD_FORM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>

namespace stan {
namespace math {
/**
 * Compute B^T A B
 **/
template <int RA, int CA, int RB, int CB, typename T>
inline Eigen::Matrix<T, CB, CB> quad_form(const Eigen::Matrix<T, RA, CA>& A,
                                          const Eigen::Matrix<T, RB, CB>& B) {
  check_square("quad_form", "A", A);
  check_multiplicable("quad_form", "A", A, "B", B);
  return B.transpose() * A * B;
}

template <int RA, int CA, int RB, typename T>
inline T quad_form(const Eigen::Matrix<T, RA, CA>& A,
                   const Eigen::Matrix<T, RB, 1>& B) {
  check_square("quad_form", "A", A);
  check_multiplicable("quad_form", "A", A, "B", B);
  return B.dot(A * B);
}

}  // namespace math
}  // namespace stan

#endif
