#ifndef STAN_MATH_PRIM_MAT_FUN_QUAD_FORM_SYM_HPP
#define STAN_MATH_PRIM_MAT_FUN_QUAD_FORM_SYM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>

namespace stan {
namespace math {

template <int RA, int CA, int RB, int CB, typename T>
inline Eigen::Matrix<T, CB, CB> quad_form_sym(
    const Eigen::Matrix<T, RA, CA>& A, const Eigen::Matrix<T, RB, CB>& B) {
  check_square("quad_form_sym", "A", A);
  check_multiplicable("quad_form_sym", "A", A, "B", B);
  check_symmetric("quad_form_sym", "A", A);
  Eigen::Matrix<T, CB, CB> ret(B.transpose() * A * B);
  return T(0.5) * (ret + ret.transpose());
}

template <int RA, int CA, int RB, typename T>
inline T quad_form_sym(const Eigen::Matrix<T, RA, CA>& A,
                       const Eigen::Matrix<T, RB, 1>& B) {
  check_square("quad_form_sym", "A", A);
  check_multiplicable("quad_form_sym", "A", A, "B", B);
  check_symmetric("quad_form_sym", "A", A);
  return B.dot(A * B);
}

}  // namespace math
}  // namespace stan
#endif
