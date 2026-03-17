#ifndef STAN_MATH_OPENCL_KERNELS_PACK_HPP
#define STAN_MATH_OPENCL_KERNELS_PACK_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* pack_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Packs a flat matrix to a packed triangular matrix
     *
     * @param[out] A packed buffer
     * @param[in] B flat matrix
     * @param rows number of columns for matrix B
     * @param cols number of columns for matrix B
     * @param part parameter that defines the triangularity of the
     * input matrix
     *  LOWER - lower triangular
     *  UPPER - upper triangular
     * if the part parameter is not specified
     * @note Code is a <code>const char*</code> held in
     * <code>pack_kernel_code.</code>
     * This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void pack(__global double* A, __global double* B,
                       unsigned int rows, unsigned int cols,
                       unsigned int part) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        // the packed matrices are stored in row major
        if (part == LOWER) {
          const int column_offset = j * rows - (j * (j - 1)) / 2;
          const int row_offset = (i - j);
          if (j <= i) {
            A[column_offset + row_offset] = B(i, j);
          }
        } else {
          const int column_offset = j * (j + 1) / 2;
          if (j >= i) {
            A[column_offset + i] = B(i, j);
          }
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/pack.hpp pack() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int, TriangularViewCL>
    pack("pack", {indexing_helpers, pack_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
