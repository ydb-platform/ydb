#ifndef STAN_MATH_OPENCL_KERNELS_ZEROS_HPP
#define STAN_MATH_OPENCL_KERNELS_ZEROS_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* zeros_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Stores zeros in the matrix on the OpenCL device.
     * Supports writing zeroes to the lower and upper triangular or
     * the whole matrix.
     *
     * @param[out] A matrix
     * @param rows Number of rows for matrix A
     * @param cols Number of columns for matrix A
     * @param part optional parameter that describes where to assign zeros:
     *  LOWER - lower triangular
     *  UPPER - upper triangular
     * if the part parameter is not specified,
     * zeros are assigned to the whole matrix.
     * @note Code is a <code>const char*</code> held in
     * <code>zeros_kernel_code.</code>
     * This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void zeros(__global double* A, unsigned int rows,
                        unsigned int cols, unsigned int part) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        if (part == LOWER && j < i) {
          A(i, j) = 0;
        } else if (part == UPPER && j > i) {
          A(i, j) = 0;
        } else if (part == ENTIRE) {
          A(i, j) = 0;
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/zeros.hpp zeros() \endlink
 */
const global_range_kernel<cl::Buffer, int, int, TriangularViewCL> zeros(
    "zeros", {indexing_helpers, zeros_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
