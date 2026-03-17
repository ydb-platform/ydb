#ifndef STAN_MATH_OPENCL_KERNELS_SCALAR_MUL_DIAGONAL_HPP
#define STAN_MATH_OPENCL_KERNELS_SCALAR_MUL_DIAGONAL_HPP
#ifdef STAN_OPENCL

#include <stan/math/opencl/kernel_cl.hpp>

namespace stan {
namespace math {
namespace opencl_kernels {
// \cond
static const char *scalar_mul_diagonal_kernel_code = STRINGIFY(
    // \endcond
    /**
     * Multiplication of the matrix A diagonal with a scalar
     *
     * @param[in, out] A matrix A
     * @param[in] scalar the value with which to multiply the diagonal of A
     * @param[in] rows the number of rows in A
     * @param[in] min_dim the size of the smaller dimension of A
     */
    __kernel void scalar_mul_diagonal(__global double *A, const double scalar,
                                      const unsigned int rows,
                                      const unsigned int min_dim) {
      int i = get_global_id(0);
      if (i < min_dim) {
        A(i, i) *= scalar;
      }
    }
    // \cond
);
// \endcond

/**
 * See the docs for \link kernels/scalar_mul_diagonal.hpp add() \endlink
 */
const global_range_kernel<cl::Buffer, double, int, int> scalar_mul_diagonal(
    "scalar_mul_diagonal", {indexing_helpers, scalar_mul_diagonal_kernel_code});

}  // namespace opencl_kernels
}  // namespace math
}  // namespace stan
#endif
#endif
