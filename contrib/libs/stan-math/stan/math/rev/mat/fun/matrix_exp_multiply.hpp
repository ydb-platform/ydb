#ifndef STAN_MATH_REV_MAT_FUN_MATRIX_EXP_MULTIPLY_HPP
#define STAN_MATH_REV_MAT_FUN_MATRIX_EXP_MULTIPLY_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/fun/matrix_exp.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/rev/mat/fun/multiply.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

/**
 * Wrapper of matrix_exp_action function for a more literal name
 * @tparam Ta scalar type matrix A
 * @tparam Tb scalar type matrix B
 * @tparam Cb Columns matrix B
 * @param[in] A Matrix
 * @param[in] B Matrix
 * @return exponential of A multiplies B
 */
template <typename Ta, typename Tb, int Cb>
inline Eigen::Matrix<typename stan::return_type<Ta, Tb>::type, -1, Cb>
matrix_exp_multiply(const Eigen::Matrix<Ta, -1, -1>& A,
                    const Eigen::Matrix<Tb, -1, Cb>& B) {
  check_nonzero_size("matrix_exp_multiply", "input matrix", A);
  check_nonzero_size("matrix_exp_multiply", "input matrix", B);
  check_multiplicable("matrix_exp_multiply", "A", A, "B", B);
  check_square("matrix_exp_multiply", "input matrix", A);
  return multiply(matrix_exp(A), B);
}

}  // namespace math
}  // namespace stan

#endif
