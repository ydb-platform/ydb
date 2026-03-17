#ifndef STAN_MATH_OPENCL_KERNELS_SCALAR_MUL_HPP
#define STAN_MATH_OPENCL_KERNELS_SCALAR_MUL_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *scalar_mul_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Multiplication of the matrix A with a scalar
     *
     * @param[in] A input matrix
     * @param[in] B output matrix
     * @param[in] scalar the value with which to multiply A
     * @param[in] rows the number of rows in A
     * @param[in] cols the number of columns in A
     */
    __kernel void scalar_mul(__global double *A, const __global double *B,
                             const double scalar, const unsigned int rows,
                             const unsigned int cols) {
      int i = get_global_id(0);
      int j = get_global_id(1);
      if (i < rows && j < cols) {
        A(i, j) = B(i, j) * scalar;
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/scalar_mul.hpp add() \endlink
 */
const global_range_kernel<cl::Buffer, cl::Buffer, double, int, int> scalar_mul(
    "scalar_mul", {indexing_helpers, scalar_mul_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
