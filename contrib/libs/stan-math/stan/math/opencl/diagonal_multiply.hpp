#ifndef STAN_MATH_OPENCL_DIAGONAL_MULTIPLY_HPP
#define STAN_MATH_OPENCL_DIAGONAL_MULTIPLY_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/opencl/kernels/scalar_mul_diagonal.hpp>
#include <Eigen/Dense>

namespace stan {
namespace math {
/**
 * Multiplies the diagonal of a matrix on the OpenCL device with the specified
 * scalar.
 *
 * @param A input matrix
 * @param scalar scalar
 * @return copy of the input matrix with the diagonal multiplied by scalar
 */
inline matrix_cl diagonal_multiply(const matrix_cl& A, const double scalar) {
  matrix_cl B(A);
  if (B.size() == 0)
    return B;
  // For rectangular matrices
  int min_dim = B.rows();
  if (B.cols() < min_dim)
    min_dim = B.cols();
  try {
    opencl_kernels::scalar_mul_diagonal(cl::NDRange(min_dim), B.buffer(),
                                        scalar, B.rows(), min_dim);
  } catch (const cl::Error& e) {
    check_opencl_error("diagonal_multiply", e);
  }
  return B;
}
}  // namespace math
}  // namespace stan

#endif
#endif
