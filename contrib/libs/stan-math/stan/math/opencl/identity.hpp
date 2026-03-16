#ifndef STAN_MATH_OPENCL_IDENTITY_HPP
#define STAN_MATH_OPENCL_IDENTITY_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/opencl/kernels/identity.hpp>
#error #include <CL/cl.hpp>

namespace stan {
namespace math {

/**
 * Returns the identity matrix stored on the OpenCL device
 *
 * @param rows_cols the number of rows and columns
 *
 * @return the identity matrix
 *
 */
inline matrix_cl identity(int rows_cols) {
  matrix_cl A(rows_cols, rows_cols);
  if (rows_cols == 0) {
    return A;
  }
  cl::CommandQueue cmdQueue = opencl_context.queue();

  try {
    opencl_kernels::identity(cl::NDRange(A.rows(), A.cols()), A.buffer(),
                             A.rows(), A.cols());
  } catch (const cl::Error& e) {
    check_opencl_error("identity", e);
  }
  return A;
}
}  // namespace math
}  // namespace stan

#endif
#endif
