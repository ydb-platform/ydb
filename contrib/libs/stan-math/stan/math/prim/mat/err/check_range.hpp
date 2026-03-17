#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_RANGE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_RANGE_HPP

#include <stan/math/prim/scal/err/out_of_range.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/meta/error_index.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if specified index is within range.
 * This check is 1-indexed by default. This behavior can be
 * changed by setting <code>stan::error_index::value</code>.
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param max Maximum size of the variable
 * @param index Index to check
 * @param nested_level Nested level (for error messages)
 * @param error_msg Additional error message (for error messages)
 * @throw <code>std::out_of_range</code> if the index is not in range
 */
inline void check_range(const char* function, const char* name, int max,
                        int index, int nested_level, const char* error_msg) {
  if ((index >= stan::error_index::value)
      && (index < max + stan::error_index::value))
    return;

  std::stringstream msg;
  msg << "; index position = " << nested_level;
  std::string msg_str(msg.str());

  out_of_range(function, max, index, msg_str.c_str(), error_msg);
}

/**
 * Check if specified index is within range.
 * This check is 1-indexed by default. This behavior can be
 * changed by setting <code>stan::error_index::value</code>.
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param max Maximum size of the variable
 * @param index Index to check
 * @param error_msg Additional error message (for error messages)
 * @throw <code>std::out_of_range</code> if the index is not in range
 */
inline void check_range(const char* function, const char* name, int max,
                        int index, const char* error_msg) {
  if ((index >= stan::error_index::value)
      && (index < max + stan::error_index::value))
    return;

  out_of_range(function, max, index, error_msg);
}

/**
 * Check if specified index is within range.
 * This check is 1-indexed by default. This behavior can be
 * changed by setting <code>stan::error_index::value</code>.
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param max Maximum size of the variable
 * @param index Index to check
 * @throw <code>std::out_of_range</code> if the index is not in range
 */
inline void check_range(const char* function, const char* name, int max,
                        int index) {
  if ((index >= stan::error_index::value)
      && (index < max + stan::error_index::value))
    return;

  out_of_range(function, max, index);
}

}  // namespace math
}  // namespace stan
#endif
