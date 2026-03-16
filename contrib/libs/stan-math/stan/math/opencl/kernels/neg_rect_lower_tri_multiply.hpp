#ifndef STAN_MATH_OPENCL_KERNELS_NEGATIVE_RECT_LOWER_TRI_MULTIPLY_HPP
#define STAN_MATH_OPENCL_KERNELS_NEGATIVE_RECT_LOWER_TRI_MULTIPLY_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char* neg_rect_lower_tri_multiply_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Calculates C = -B * A where B is rectangular and A is a lower
     * triangular.
     *  For a full guide to the inverse lower triangular kernels see the link
     * <a
     href="https://github.com/stan-dev/math/wiki/(OpenCL)-Kernels">here</a>.
     *
     * ![Inverse Calculation](https://goo.gl/6jBjEG)
     *
     * Graphically, this kernel calculates `-temp * C1` where temp is the
     *   C2 * A3 calculation from
     *  \link kernels/inv_lower_tri_multiply.hpp inv_lower_tri_multiply()
     \endlink
     * The kernel is executed using (N, N, m) threads, where N is the size of
     *  the input matrices.

     * @param[in, out] A Input matrix that is being inverted.
     * @param[in] temp Temporary matrix with the intermediate results.
     * @param A_rows Number of rows for A.
     * @param rows The number of rows in a single matrix of the batch
     * @note Code is a <code>const char*</code> held in
     *  neg_rect_lower_tri_multiply_kernel_code
     *  Used in math/opencl/lower_tri_inverse.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void neg_rect_lower_tri_multiply(
        __global double* A, const __global double* temp, const int A_rows,
        const int rows) {
      int result_matrix_id = get_global_id(2);
      int offset = result_matrix_id * rows * 2;
      const int thread_block_row = get_local_id(0);
      const int thread_block_col = get_local_id(1);
      const int i = THREAD_BLOCK_SIZE * get_group_id(0) + thread_block_row;
      const int j = THREAD_BLOCK_SIZE * get_group_id(1) + thread_block_col;

      __local double temp_local[THREAD_BLOCK_SIZE][THREAD_BLOCK_SIZE];
      __local double C1_local[THREAD_BLOCK_SIZE][THREAD_BLOCK_SIZE];

      double acc[WORK_PER_THREAD] = {0};

      const int num_tiles = (rows + THREAD_BLOCK_SIZE - 1) / THREAD_BLOCK_SIZE;
      for (int tile_ind = 0; tile_ind < num_tiles; tile_ind++) {
        // each thread copies WORK_PER_THREAD values to the local
        // memory
        for (int w = 0; w < WORK_PER_THREAD; w++) {
          const int tiled_i = THREAD_BLOCK_SIZE * tile_ind + thread_block_row;
          const int tiled_j = THREAD_BLOCK_SIZE * tile_ind + thread_block_col;
          const int temp_global_col = tiled_j + w * THREAD_BLOCK_SIZE_COL;
          // {C2}{A2}_global_{col}{row} specifies which global element for each
          // matrix the thread is in charge of moving to local memory.
          const int C1_global_col = offset + j + w * THREAD_BLOCK_SIZE_COL;
          const int C1_global_row = tiled_i + offset;
          // Which {col}{row} location in the local memory the thread is in
          //  charge of.
          const int local_col = thread_block_col + w * THREAD_BLOCK_SIZE_COL;
          const int local_row = thread_block_row;
          if ((temp_global_col) < rows && i < rows) {
            temp_local[local_col][local_row]
                = temp[result_matrix_id * rows * rows + temp_global_col * rows
                       + i];
          } else {
            temp_local[local_col][local_row] = 0.0;
          }
          // Element above the diagonal will not be transferred.
          if (C1_global_col <= C1_global_row && C1_global_col < A_rows
              && C1_global_row < A_rows) {
            C1_local[local_col][local_row]
                = A[C1_global_col * A_rows + C1_global_row];
          } else {
            C1_local[local_col][local_row] = 0;
          }
        }
        // wait until all tile values are loaded to the local memory
        barrier(CLK_LOCAL_MEM_FENCE);
        for (int block_ind = 0; block_ind < THREAD_BLOCK_SIZE; block_ind++) {
          for (int w = 0; w < WORK_PER_THREAD; w++) {
            // Which {col}{row} location in the local memory the thread is in
            //  charge of.
            const int local_col = thread_block_col + w * THREAD_BLOCK_SIZE_COL;
            const int local_row = thread_block_row;
            acc[w] += temp_local[block_ind][local_row]
                      * C1_local[local_col][block_ind];
          }
        }
        barrier(CLK_LOCAL_MEM_FENCE);
      }
      // A_global_{row}{col} tells the thread which local memory it needs
      //  to move to the final output
      const int A_global_row = i + rows + offset;
      const int A_global_col_offset = offset + j;
      // each thread saves WORK_PER_THREAD values
      for (int w = 0; w < WORK_PER_THREAD; w++) {
        const int A_global_col
            = A_global_col_offset + w * THREAD_BLOCK_SIZE_COL;
        if (A_global_col < A_rows && (i + rows + offset) < A_rows) {
          A[A_global_col * A_rows + i + rows + offset] = -acc[w];
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs
 * for \link kernels/neg_rect_lower_tri_multiply.hpp
 * neg_rect_lower_tri_multiply() \endlink
 */
const local_range_kernel<cl::Buffer, cl::Buffer, int, int>
    neg_rect_lower_tri_multiply("neg_rect_lower_tri_multiply",
                                {thread_block_helpers,
                                 neg_rect_lower_tri_multiply_kernel_code},
                                {{"THREAD_BLOCK_SIZE", 32},
                                 {"WORK_PER_THREAD", 8}});
}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
