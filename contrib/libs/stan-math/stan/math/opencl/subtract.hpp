#ifndef STAN_MATH_OPENCL_SUBTRACT_HPP
#define STAN_MATH_OPENCL_SUBTRACT_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/opencl/kernels/subtract.hpp>
#include <stan/math/opencl/err/check_matching_dims.hpp>
#error #include <CL/cl.hpp>

namespace stan {
namespace math {

/**
 * Matrix subtraction on the OpenCL device
 * Subtracts the second matrix
 * from the first matrix and stores
 * the result in the third matrix (C=A-B)
 *
 * @param A first matrix
 * @param B second matrix
 *
 * @return subtraction result matrix
 *
 * @throw <code>std::invalid_argument</code> if the
 * input matrices do not have matching dimensions.
 *
 */
inline auto subtract(const matrix_cl& A, const matrix_cl& B) {
  check_matching_dims("subtract ((OpenCL))", "A", A, "B", B);
  matrix_cl C(A.rows(), A.cols());
  if (A.size() == 0) {
    return C;
  }
  cl::CommandQueue cmdQueue = opencl_context.queue();
  try {
    opencl_kernels::subtract(cl::NDRange(A.rows(), A.cols()), C.buffer(),
                             A.buffer(), B.buffer(), A.rows(), A.cols());
  } catch (cl::Error& e) {
    check_opencl_error("subtract", e);
  }
  return C;
}

/**
 * Matrix subtraction on the OpenCL device
 * Subtracts the second matrix
 * from the first matrix and stores
 * the result in the third matrix (C=A-B)
 *
 * @param A first matrix
 * @param B second matrix
 *
 * @return subtraction result matrix
 *
 * @throw <code>std::invalid_argument</code> if the
 * input matrices do not have matching dimensions.
 *
 */
inline auto operator-(const matrix_cl& A, const matrix_cl& B) {
  return subtract(A, B);
}
}  // namespace math
}  // namespace stan

#endif
#endif
