#ifndef STAN_MATH_OPENCL_KERNELS_TRIANGULAR_TRANSPOSE_HPP
#define STAN_MATH_OPENCL_KERNELS_TRIANGULAR_TRANSPOSE_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* triangular_transpose_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Copies a lower/upper triangular of a matrix to it's upper/lower.
     *
     * @param[in,out] A The matrix.
     * @param rows The number of rows in A.
     * @param cols The number of cols in A.
     * @param copy_direction A value of zero or one specifying
     *  which direction to copy
     *  LOWER_TO_UPPER: 1
     *  UPPER_TO_LOWER: 0
     * @note Code is a <code>const char*</code> held in
     * <code>triangular_transpose_kernel_code.</code>
     * Used in mat/opencl/triangular_transpose.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void triangular_transpose(__global double* A, unsigned int rows,
                                       unsigned int cols,
                                       unsigned int copy_direction) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        if (copy_direction == LOWER_TO_UPPER && i > j) {
          A(j, i) = A(i, j);
        } else if (copy_direction == UPPER_TO_LOWER && i > j) {
          A(i, j) = A(j, i);
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/triangular_transpose.hpp
 * triangular_transpose() \endlink
 */
const global_range_kernel<cl::Buffer, int, int, TriangularMapCL>
    triangular_transpose("triangular_transpose",
                         {indexing_helpers, triangular_transpose_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
