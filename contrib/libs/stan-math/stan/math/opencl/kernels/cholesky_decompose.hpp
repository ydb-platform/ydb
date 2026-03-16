#ifndef STAN_MATH_OPENCL_KERNELS_CHOLESKY_DECOMPOSE_HPP
#define STAN_MATH_OPENCL_KERNELS_CHOLESKY_DECOMPOSE_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *cholesky_decompose_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Calculates the Cholesky Decomposition of a matrix on an OpenCL
     *
     * This kernel is run with threads organized in one dimension and
     * in a single thread block. The kernel is best suited for
     * small input matrices as it only utilizes a single streaming
     * multiprocessor. The kernels is used as a part of a blocked
     * cholesky decompose.
     *
     * @param[in] A The input matrix
     * @param[in, out] B The result of cholesky decompositon of A.
     * @param rows The number of rows for A and B.
     * @note Code is a <code>const char*</code> held in
     * <code>cholesky_decompose_kernel_code.</code>
     *  Used in math/opencl/cholesky_decompose.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     *
     */
    __kernel void cholesky_decompose(__global double *A, __global double *B,
                                     int rows) {
      int local_index = get_local_id(0);
      // Fill B with zeros
      // B is square so checking row length is fine for both i and j
      if (local_index < rows) {
        for (int k = 0; k < rows; k++) {
          B(local_index, k) = 0;
        }
      }
      // The following code is the sequential version of the non-inplace
      // cholesky decomposition. Only the innermost loops are parallelized. The
      // rows are processed sequentially. This loop process all the rows:
      for (int j = 0; j < rows; j++) {
        // First thread calculates the diagonal element
        if (local_index == 0) {
          double sum = 0;
          for (int k = 0; k < j; k++) {
            sum += B(j, k) * B(j, k);
          }
          B(j, j) = sqrt(A(j, j) - sum);
        }
        barrier(CLK_LOCAL_MEM_FENCE);
        // Calculates the rest of the row
        if (local_index >= (j + 1) && local_index < rows) {
          double inner_sum = 0;
          for (int k = 0; k < j; k++) {
            inner_sum += B(local_index, k) * B(j, k);
          }
          B(local_index, j) = (1.0 / B(j, j) * (A(local_index, j) - inner_sum));
        }
        barrier(CLK_LOCAL_MEM_FENCE);
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/cholesky_decompose.hpp cholesky_decompose()
 * \endlink
 */
const local_range_kernel<cl::Buffer, cl::Buffer, int> cholesky_decompose(
    "cholesky_decompose", {indexing_helpers, cholesky_decompose_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
