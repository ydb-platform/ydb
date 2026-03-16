#ifndef STAN_MATH_REV_MAT_FUN_SCALE_MATRIX_EXP_MULTIPLY_HPP
#define STAN_MATH_REV_MAT_FUN_SCALE_MATRIX_EXP_MULTIPLY_HPP

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
 * Return product of exp(At) and B, where A is a NxN matrix,
 * B is a NxCb matrix, and t is a double
 *
 * @tparam Ta scalar type matrix A
 * @tparam Tb scalar type matrix B
 * @tparam Cb Columns matrix B
 * @param[in] A Matrix
 * @param[in] B Matrix
 * @param[in] t double
 * @return exponential of At multiplies B
 */
template <typename Ta, typename Tb, int Cb>
inline Eigen::Matrix<typename stan::return_type<Ta, Tb>::type, -1, Cb>
scale_matrix_exp_multiply(const double& t, const Eigen::Matrix<Ta, -1, -1>& A,
                          const Eigen::Matrix<Tb, -1, Cb>& B) {
  check_nonzero_size("scale_matrix_exp_multiply", "input matrix", A);
  check_nonzero_size("scale_matrix_exp_multiply", "input matrix", B);
  check_multiplicable("scale_matrix_exp_multiply", "A", A, "B", B);
  check_square("scale_matrix_exp_multiply", "input matrix", A);
  return multiply(matrix_exp(multiply(A, t)), B);
}

}  // namespace math
}  // namespace stan

#endif
