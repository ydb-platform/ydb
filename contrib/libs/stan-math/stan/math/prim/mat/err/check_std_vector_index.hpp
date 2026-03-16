#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_STD_VECTOR_INDEX_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_STD_VECTOR_INDEX_HPP

#include <stan/math/prim/scal/err/out_of_range.hpp>

#include <sstream>
#include <string>
#include <vector>

namespace stan {
namespace math {

/**
 * Check if the specified index is valid in std vector
 * This check is 1-indexed by default. This behavior can be changed
 * by setting <code>stan::error_index::value</code>.
 * @tparam T Scalar type
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y <code>std::vector</code> to test
 * @param i Index
 * @throw <code>std::out_of_range</code> if the index is out of range.
 */
template <typename T>
inline void check_std_vector_index(const char* function, const char* name,
                                   const std::vector<T>& y, int i) {
  if (i >= static_cast<int>(stan::error_index::value)
      && i < static_cast<int>(y.size() + stan::error_index::value))
    return;

  std::stringstream msg;
  msg << " for " << name;
  std::string msg_str(msg.str());
  out_of_range(function, y.size(), i, msg_str.c_str());
}

}  // namespace math
}  // namespace stan
#endif
