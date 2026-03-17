#ifndef STAN_MATH_OPENCL_KERNELS_CHECK_SYMMETRIC_HPP
#define STAN_MATH_OPENCL_KERNELS_CHECK_SYMMETRIC_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *is_symmetric_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Check if the <code>matrix_cl</code> is symmetric
     *
     * @param[in] A The matrix to check.
     * @param rows The number of rows in matrix A.
     * @param cols The number of columns in matrix A.
     * @param[out] flag the flag to be written to if any diagonal is zero.
     * @param tolerance The numerical tolerance to check wheter
     *   two values are equal
     * @note Code is a <code>const char*</code> held in
     * <code>is_symmetric_kernel_code.</code>
     * Kernel for stan/math/opencl/err/check_symmetric.hpp.
     *  This kernel uses the helper macros available in helpers.cl.
     */
    __kernel void is_symmetric(__global double *A, __global int *flag,
                               unsigned int rows, unsigned int cols,
                               double tolerance) {
      const int i = get_global_id(0);
      const int j = get_global_id(1);
      if (i < rows && j < cols) {
        double diff = fabs(A(i, j) - A(j, i));
        if (diff > tolerance) {
          flag[0] = 0;
        }
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/check_symmetric.hpp check_symmetric() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, int, int, const double>
    check_symmetric("is_symmetric",
                    {indexing_helpers, is_symmetric_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
