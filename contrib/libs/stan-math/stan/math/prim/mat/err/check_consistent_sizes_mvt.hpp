#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_CONSISTENT_SIZES_MVT_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_CONSISTENT_SIZES_MVT_HPP

#include <stan/math/prim/mat/err/check_consistent_size_mvt.hpp>
#include <stan/math/prim/mat/meta/length_mvt.hpp>
#include <algorithm>

namespace stan {
namespace math {

/**
 * Check if the dimension of x1 is consistent with x2.
 * Consistent size is defined as having the same size if vector of
 * vectors or being a single vector.
 * @tparam T1 Type of x1
 * @tparam T2 Type of x2
 * @param function Function name (for error messages)
 * @param name1 Variable name (for error messages)
 * @param x1 Variable to check for consistent size
 * @param name2 Variable name (for error messages)
 * @param x2 Variable to check for consistent size
 * @throw <code>invalid_argument</code> if sizes are inconsistent
 */
template <typename T1, typename T2>
inline void check_consistent_sizes_mvt(const char* function, const char* name1,
                                       const T1& x1, const char* name2,
                                       const T2& x2) {
  using stan::length_mvt;
  size_t max_size = std::max(length_mvt(x1), length_mvt(x2));
  check_consistent_size_mvt(function, name1, x1, max_size);
  check_consistent_size_mvt(function, name2, x2, max_size);
}

}  // namespace math
}  // namespace stan
#endif
