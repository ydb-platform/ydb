#ifndef STAN_MATH_OPENCL_KERNELS_SUB_BLOCK_HPP
#define STAN_MATH_OPENCL_KERNELS_SUB_BLOCK_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *sub_block_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Copies a submatrix of the source matrix to
     * the destination matrix. The submatrix to copy
     * starts at (0, 0)
     * and is of size size_rows x size_cols.
     * The submatrix is copied to the
     * destination matrix starting at
     * (dst_offset_rows, dst_offset_cols)
     *
     * @param[in] src The source matrix.
     * @param[out] dst The destination submatrix.
     * @param src_offset_i The offset row in src.
     * @param src_offset_j The offset column in src.
     * @param dst_offset_i The offset row in dst.
     * @param dst_offset_j The offset column in dst.
     * @param size_i The number of rows in the submatrix.
     * @param size_j The number of columns in the submatrix.
     * @param src_rows The number of rows in the source matrix.
     * @param src_cols The number of cols in the source matrix.
     * @param src_rows The number of rows in the destination matrix.
     * @param dst_cols The number of cols in the destination matrix.
     * @param dst_rows The number of rows in the destination matrix.
     * @param triangular_view the triangularity of src (lower, upper or none)
     * @note Code is a <code>const char*</code> held in
     * <code>sub_block_kernel_code.</code>
     * Used in math/opencl/copy_submatrix_opencl.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     *
     */
    __kernel void sub_block(
        __global double *src, __global double *dst, unsigned int src_offset_i,
        unsigned int src_offset_j, unsigned int dst_offset_i,
        unsigned int dst_offset_j, unsigned int size_i, unsigned int size_j,
        unsigned int src_rows, unsigned int src_cols, unsigned int dst_rows,
        unsigned int dst_cols, unsigned int triangular_view) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if ((i + src_offset_i) < src_rows && (j + src_offset_j) < src_cols
          && (i + dst_offset_i) < dst_rows && (j + dst_offset_j) < dst_cols) {
        if ((triangular_view == LOWER && i >= j)
            || (triangular_view == UPPER && i <= j)
            || triangular_view == ENTIRE) {
          dst((dst_offset_i + i), (dst_offset_j + j))
              = src((src_offset_i + i), (src_offset_j + j));
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/sub_block.hpp sub_block() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int, int, int, int, int,
                          int, int, int, int, TriangularViewCL>
    sub_block("sub_block", {indexing_helpers, sub_block_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
