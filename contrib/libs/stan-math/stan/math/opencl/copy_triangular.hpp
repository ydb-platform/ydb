#ifndef STAN_MATH_OPENCL_COPY_TRIANGULAR_HPP
#define STAN_MATH_OPENCL_COPY_TRIANGULAR_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/constants.hpp>
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/opencl/copy.hpp>
#include <stan/math/opencl/kernels/copy_triangular.hpp>
#error #include <CL/cl.hpp>

namespace stan {
namespace math {

/**
 * Copies the lower or upper
 * triangular of the source matrix to
 * the destination matrix.
 * Both matrices are stored on the OpenCL device.
 *
 * @param src the source matrix
 * @tparam triangular_map int to describe
 * which part of the matrix to copy:
 * TriangularViewCL::Lower - copies the lower triangular
 * TriangularViewCL::Upper - copes the upper triangular
 *
 * @return the matrix with the copied content
 *
 */
template <TriangularViewCL triangular_view = TriangularViewCL::Entire>
inline matrix_cl copy_triangular(const matrix_cl& src) {
  if (src.size() == 0 || src.size() == 1) {
    matrix_cl dst(src);
    return dst;
  }
  matrix_cl dst(src.rows(), src.cols());
  cl::CommandQueue cmdQueue = opencl_context.queue();
  try {
    opencl_kernels::copy_triangular(cl::NDRange(dst.rows(), dst.cols()),
                                    dst.buffer(), src.buffer(), dst.rows(),
                                    dst.cols(), triangular_view);
  } catch (const cl::Error& e) {
    check_opencl_error("copy_triangular", e);
  }
  return dst;
}
}  // namespace math
}  // namespace stan

#endif
#endif
