#ifndef STAN_MATH_OPENCL_KERNELS_TRANSPOSE_HPP
#define STAN_MATH_OPENCL_KERNELS_TRANSPOSE_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *transpose_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Takes the transpose of the matrix on the OpenCL device.
     *
     * @param[out] B The output matrix to hold transpose of A.
     * @param[in] A The input matrix to transpose into B.
     * @param rows The number of rows for A.
     * @param cols The number of columns for A.
     * @note Code is a <code>const char*</code> held in
     * <code>transpose_kernel_code.</code>
     * This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void transpose(__global double *B, __global double *A,
                            unsigned int rows, unsigned int cols) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        BT(j, i) = A(i, j);
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/transpose.hpp transpose() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int> transpose(
    "transpose", {indexing_helpers, transpose_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
