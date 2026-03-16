#ifndef STAN_MATH_PRIM_ARR_ERR_CHECK_MATCHING_SIZES_HPP
#define STAN_MATH_PRIM_ARR_ERR_CHECK_MATCHING_SIZES_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>

namespace stan {
namespace math {

/**
 * Check if two structures at the same size.
 * This function only checks the runtime sizes for variables that
 * implement a <code>size()</code> method.
 * @tparam T_y1 Type of the first variable
 * @tparam T_y2 Type of the second variable
 * @param function Function name (for error messages)
 * @param name1 First variable name  (for error messages)
 * @param y1 First variable
 * @param name2 Second variable name (for error messages)
 * @param y2 Second variable
 * @throw <code>std::invalid_argument</code> if the sizes do not match
 */
template <typename T_y1, typename T_y2>
inline void check_matching_sizes(const char* function, const char* name1,
                                 const T_y1& y1, const char* name2,
                                 const T_y2& y2) {
  check_size_match(function, "size of ", name1, y1.size(), "size of ", name2,
                   y2.size());
}

}  // namespace math
}  // namespace stan
#endif
