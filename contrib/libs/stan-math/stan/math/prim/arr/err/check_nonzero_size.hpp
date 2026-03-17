#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_NONZERO_SIZE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_NONZERO_SIZE_HPP

#include <stan/math/prim/scal/err/invalid_argument.hpp>

namespace stan {
namespace math {

/**
 * Check if the specified matrix/vector is of non-zero size.
 * Throws a std:invalid_argument otherwise. The message
 * will indicate that the variable name "has size 0".
 * @tparam T_y Type of container
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Container to test. This will accept matrices and vectors
 * @throw <code>std::invalid_argument</code> if the specified matrix/vector
 *   has zero size
 */
template <typename T_y>
inline void check_nonzero_size(const char* function, const char* name,
                               const T_y& y) {
  if (y.size() > 0)
    return;
  invalid_argument(function, name, 0, "has size ",
                   ", but must have a non-zero size");
}

}  // namespace math
}  // namespace stan
#endif
