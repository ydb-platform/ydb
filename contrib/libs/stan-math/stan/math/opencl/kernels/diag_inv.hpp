#ifndef STAN_MATH_OPENCL_KERNELS_DIAGONAL_INVERSE_LOWER_TRI_HPP
#define STAN_MATH_OPENCL_KERNELS_DIAGONAL_INVERSE_LOWER_TRI_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* diag_inv_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Calculates inplace submatrix inversions along the matrix diagonal.
     *
     *  For a full guide to the inverse lower triangular kernels see the link
     * <a
     * href="https://github.com/stan-dev/math/wiki/(OpenCL)-Kernels">here</a>.
     * In the special case that the thread block size is larger than the input
     * matrix A then this kernel will perform the complete lower triangular
     * of matrix A. More often, TB is smaller than A and A will have lower
     * triangular inverses calculated on submatrices along the diagonal equal to
     * the size of the thread block. Running this kernel on a matrix with N = 4
     * * thread_block will yield a lower triangular matrix with identity
     * matrices in blue as shown below.
     * ![Identity matrices in the blue triangles](https://goo.gl/Fz2tRi)
     *
     * This kernel is run with threads organized in a single dimension.
     *  If we want to calculate N blocks of size TB across the diagonal
     *  we spawn N x TB threads with TB used as the thread block size.
     *
     * @param[in,out] A The input matrix.
     * @param[in, out] tmp_inv A matrix with batches of identities matrices
     *  along the diagonal.
     * @param rows The number of rows for A.
     * @note Code is a <code>const char*</code> held in
     * <code>diag_inv_kernel_code.</code>
     *  Used in math/opencl/lower_tri_inverse.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void diag_inv(__global double* A, __global double* tmp_inv,
                           int rows) {
      int index = get_local_id(0);
      int group = get_group_id(0);
      int block_size = get_local_size(0);
      int A_offset = group * block_size;
      // offset inside the matrix with batched identities
      int tmp_offset = group * block_size * block_size + index * block_size;

      // The following code is the sequential version of forward
      // substitution with the identity matrix as RHS. Only the innermost loops
      // are parallelized. The rows are processed sequentially. This loop
      // process all the rows:
      for (int k = 0; k < block_size; k++) {
        double diag_ele = A(A_offset + k, A_offset + k);

        // Each element under the diagonal of the RHS is divided by diag_ele.
        // Each thread in a thread block does 1 division.
        // Threads that are assigned elements above the diagonal
        // skip this division.
        if (index <= k) {
          tmp_inv[tmp_offset + k] /= diag_ele;
        }
        barrier(CLK_LOCAL_MEM_FENCE);
        // Each thread updates one column in the RHS matrix
        // (ignores values above the diagonal).
        for (int i = max(k + 1, index); i < block_size; i++) {  // NOLINT
          double factor = A(A_offset + i, A_offset + k);
          tmp_inv[tmp_offset + i] -= tmp_inv[tmp_offset + k] * factor;
        }
        barrier(CLK_LOCAL_MEM_FENCE);
      }
      for (int j = 0; j < block_size; j++) {
        // Each thread copies one column.
        A(A_offset + j, A_offset + index) = tmp_inv[tmp_offset + j];
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/diag_inv.hpp add()
 * \endlink
 */
const local_range_kernel<cl::Buffer, cl::Buffer, int> diag_inv(
    "diag_inv", {indexing_helpers, diag_inv_kernel_code},
    {{"THREAD_BLOCK_SIZE", 32}});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
