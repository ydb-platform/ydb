#ifndef STAN_MATH_OPENCL_ERR_CHECK_SYMMETRIC_HPP
#define STAN_MATH_OPENCL_ERR_CHECK_SYMMETRIC_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/opencl/err/check_square.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/opencl/kernels/check_symmetric.hpp>

namespace stan {
namespace math {
/**
 * Check if the <code>matrix_cl</code> is symmetric
 *
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y <code>matrix_cl</code> to test
 *
 * @throw <code>std::domain_error</code> if
 *    the matrix is not symmetric.
 */
inline void check_symmetric(const char* function, const char* name,
                            const matrix_cl& y) {
  if (y.size() == 0)
    return;
  check_square(function, name, y);
  cl::CommandQueue cmd_queue = opencl_context.queue();
  cl::Context& ctx = opencl_context.context();
  try {
    int symmetric_flag = 1;
    cl::Buffer buffer_symmetric_flag(ctx, CL_MEM_READ_WRITE, sizeof(int));
    cmd_queue.enqueueWriteBuffer(buffer_symmetric_flag, CL_TRUE, 0, sizeof(int),
                                 &symmetric_flag);
    opencl_kernels::check_symmetric(cl::NDRange(y.rows(), y.cols()), y.buffer(),
                                    buffer_symmetric_flag, y.rows(), y.cols(),
                                    math::CONSTRAINT_TOLERANCE);
    cmd_queue.enqueueReadBuffer(buffer_symmetric_flag, CL_TRUE, 0, sizeof(int),
                                &symmetric_flag);
    //  if the matrix is not symmetric
    if (!symmetric_flag) {
      domain_error(function, name, "is not symmetric", "");
    }
  } catch (const cl::Error& e) {
    check_opencl_error("symmetric_check", e);
  }
}

}  // namespace math
}  // namespace stan
#endif
#endif
