#ifndef STAN_MATH_OPENCL_ERR_CHECK_SQUARE_HPP
#define STAN_MATH_OPENCL_ERR_CHECK_SQUARE_HPP
#ifdef STAN_OPENCL
#include <stan/math/opencl/matrix_cl.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>

namespace stan {
namespace math {
/**
 * Check if the <code>matrix_cl</code> is square.
 *
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y <code>matrix_cl</code> to test
 *
 * @throw <code>std::invalid_argument</code> if the <code>matrix_cl</code>
 *    is not square
 */
inline void check_square(const char* function, const char* name,
                         const matrix_cl& y) {
  check_size_match(function, "Expecting a square matrix; rows of ", name,
                   y.rows(), "columns of ", name, y.cols());
}

}  // namespace math
}  // namespace stan
#endif
#endif
