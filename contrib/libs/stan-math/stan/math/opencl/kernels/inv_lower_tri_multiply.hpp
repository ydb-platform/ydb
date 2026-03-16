#ifndef STAN_MATH_OPENCL_KERNELS_INVERSE_LOWER_TRI_MULTIPLY_HPP
#define STAN_MATH_OPENCL_KERNELS_INVERSE_LOWER_TRI_MULTIPLY_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* inv_lower_tri_multiply_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Calculates B = C * A. C is an inverse matrix and A is lower triangular.
     *
     * This kernel is used in the final iteration of the batched lower
     * triangular inversion.
     *  For a full guide to the inverse lower triangular kernels see the link
     * <a
     * href="https://github.com/stan-dev/math/wiki/(OpenCL)-Kernels">here</a>.
     * The full inverse requires calculation of the lower left rectangular
     * matrix within the lower left triangular C3 = -C2*A3*C1. where C2 is the
     * inverse of the bottom right lower triangular, C1 is the inverse of the
     * upper left lower and A3 is the original lower triangulars lower left
     * rectangular. This kernel takes the output from
     * <code>neg_rect_lower_tri_multiply</code> and applies
     * the submatrix multiplcation to get the final output for C3.
     * ![Inverse Calculation](https://goo.gl/6jBjEG)
     *
     * Graphically, this kernel calculates the C2 * A3.
     * The kernel is executed using (N, N, m) threads, where N is the size of
     *  the input matrices.
     *
     * @param[in] A input matrix that is being inverted.
     * @param[out] temp output matrix with results of the batched matrix
     * multiplications
     * @param A_rows The number of rows for A.
     * @param rows The number of rows in a single matrix of the batch
     * @note Code is a <code>const char*</code> held in
     * <code>inv_lower_tri_multiply_kernel_code.</code>
     *  Used in math/opencl/lower_tri_inverse.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void inv_lower_tri_multiply(__global double* A,
                                         __global double* temp,
                                         const int A_rows, const int rows) {
      int result_matrix_id = get_global_id(2);
      int offset = result_matrix_id * rows * 2;
      const int thread_block_row = get_local_id(0);
      const int thread_block_col = get_local_id(1);
      const int global_thread_row
          = THREAD_BLOCK_SIZE * get_group_id(0) + thread_block_row;
      const int global_thread_col
          = THREAD_BLOCK_SIZE * get_group_id(1) + thread_block_col;

      __local double C2_local[THREAD_BLOCK_SIZE][THREAD_BLOCK_SIZE];
      __local double A3_local[THREAD_BLOCK_SIZE][THREAD_BLOCK_SIZE];

      double acc[WORK_PER_THREAD] = {0};

      const int num_tiles = (rows + THREAD_BLOCK_SIZE - 1) / THREAD_BLOCK_SIZE;
      for (int tile_ind = 0; tile_ind < num_tiles; tile_ind++) {
        // Each thread copies WORK_PER_THREAD values to the local
        // memory
        for (int w = 0; w < WORK_PER_THREAD; w++) {
          const int tiled_i = THREAD_BLOCK_SIZE * tile_ind + thread_block_row;
          const int tiled_j = THREAD_BLOCK_SIZE * tile_ind + thread_block_col;
          // {C2}{A2}_global_{col}{row} specifies which global element for each
          // matrix the thread is in charge of moving to local memory.
          const int C2_global_col
              = offset + rows + tiled_j + w * THREAD_BLOCK_SIZE_COL;
          const int C2_global_row = offset + global_thread_row + rows;
          const int A3_global_col
              = offset + global_thread_col + w * THREAD_BLOCK_SIZE_COL;
          const int A3_global_row = tiled_i + rows + offset;
          // Which {col}{row} location in the local memory the thread is in
          //  charge of.
          const int local_col = thread_block_col + w * THREAD_BLOCK_SIZE_COL;
          const int local_row = thread_block_row;
          // Element above the diagonal will not be transferred.
          if (C2_global_col <= C2_global_row && C2_global_col < A_rows
              && C2_global_row < A_rows) {
            C2_local[local_col][local_row]
                = A[C2_global_col * A_rows + C2_global_row];
          } else {
            C2_local[local_col][local_row] = 0;
          }
          if (A3_global_col < A_rows && A3_global_row < A_rows) {
            A3_local[local_col][local_row]
                = A[A3_global_col * A_rows + A3_global_row];
          } else {
            A3_local[local_col][local_row] = 0.0;
          }
        }
        // Wait until all tile values are loaded to the local memory
        barrier(CLK_LOCAL_MEM_FENCE);
        for (int block_ind = 0; block_ind < THREAD_BLOCK_SIZE; block_ind++) {
          for (int w = 0; w < WORK_PER_THREAD; w++) {
            const int local_col = thread_block_col + w * THREAD_BLOCK_SIZE_COL;
            const int local_row = thread_block_row;
            acc[w] += C2_local[block_ind][local_row]
                      * A3_local[local_col][block_ind];
          }
        }
        barrier(CLK_LOCAL_MEM_FENCE);
      }
      // Global offset for each resulting submatrix
      const int batch_offset = result_matrix_id * rows * rows;
      // temp_global_{row}{col} tells the thread which local memory it needs
      //  to move to the final output
      const int temp_global_row = global_thread_row;
      // save the values
      for (int w = 0; w < WORK_PER_THREAD; w++) {
        // each thread saves WORK_PER_THREAD values
        const int temp_global_col
            = global_thread_col + w * THREAD_BLOCK_SIZE_COL;
        temp[batch_offset + temp_global_col * rows + temp_global_row] = acc[w];
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/inv_lower_tri_multiply.hpp add() \endlink
 */
const local_range_kernel<cl::Buffer, cl::Buffer, int, int>
    inv_lower_tri_multiply("inv_lower_tri_multiply",
                           {thread_block_helpers,
                            inv_lower_tri_multiply_kernel_code},
                           {{"THREAD_BLOCK_SIZE", 32}, {"WORK_PER_THREAD", 8}});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
