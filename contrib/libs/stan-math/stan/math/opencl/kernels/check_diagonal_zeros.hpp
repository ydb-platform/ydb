#ifndef STAN_MATH_OPENCL_KERNELS_CHECK_DIAGONAL_ZEROS_HPP
#define STAN_MATH_OPENCL_KERNELS_CHECK_DIAGONAL_ZEROS_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *is_zero_on_diagonal_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Check if the <code>matrix_cl</code> has zeros on the diagonal
     *
     * @param[in] A Matrix to check.
     * @param[out] flag the flag to be written to if any diagonal is zero.
     * @param rows The number of rows for A.
     * @param cols The number of cols of A.
     * @note Code is a <code>const char*</code> held in
     * <code>is_zero_on_diagonal_kernel_code.</code>
     * Kernel for stan/math/opencl/err/check_diagonal_zeros.hpp.
     * This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void is_zero_on_diagonal(__global double *A, __global int *flag,
                                      unsigned int rows, unsigned int cols) {
      const int i = get_global_id(0);
      if (i < rows && i < cols) {
        if (A(i, i) == 0) {
          flag[0] = 1;
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/check_diagonal_zeros.hpp
 * check_diagonal_zeros() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int>
    check_diagonal_zeros("is_zero_on_diagonal",
                         {indexing_helpers, is_zero_on_diagonal_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
