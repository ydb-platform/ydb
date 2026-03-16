#ifndef STAN_MATH_OPENCL_KERNELS_COPY_TRIANGULAR_HPP
#define STAN_MATH_OPENCL_KERNELS_COPY_TRIANGULAR_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *copy_triangular_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Copies the lower or upper
     * triangular of the source matrix to
     * the destination matrix.
     * Both matrices are stored on the OpenCL device.
     *
     * @param[out] A Output matrix to copy triangular to.
     * @param[in] B The matrix to copy the triangular from.
     * @param rows The number of rows of B.
     * @param cols The number of cols of B.
     * @param lower_upper determines
     * which part of the matrix to copy:
     *  LOWER: 0 - copies the lower triangular
     *  UPPER: 1 - copes the upper triangular
     * @note Code is a <code>const char*</code> held in
     * <code>copy_triangular_kernel_code.</code>
     * Used in math/opencl/copy_triangular_opencl.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void copy_triangular(__global double *A, __global double *B,
                                  unsigned int rows, unsigned int cols,
                                  unsigned int lower_upper) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        if (!lower_upper && j <= i) {
          A(i, j) = B(i, j);
        } else if (!lower_upper) {
          A(i, j) = 0;
        } else if (lower_upper && j >= i) {
          A(i, j) = B(i, j);
        } else if (lower_upper && j < i) {
          A(i, j) = 0;
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/copy_triangular.hpp copy_triangular() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int, TriangularViewCL>
    copy_triangular("copy_triangular",
                    {indexing_helpers, copy_triangular_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
