#ifndef STAN_MATH_PRIM_SCAL_ERR_OUT_OF_RANGE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_OUT_OF_RANGE_HPP

#include <stan/math/prim/scal/meta/error_index.hpp>
#include <typeinfo>
#include <sstream>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Throw an out_of_range exception with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * out of range. This will allow us to change the behavior for all
 * functions at once.
 * The message is:
 * "<function>: index <index> out of range; expecting index to be between "
 * "1 and <max><msg1><msg2>"
 * @param function Name of the function
 * @param max Max
 * @param index Index
 * @param msg1 Message to print. Default is "".
 * @param msg2 Message to print. Default is "".
 * @throw std::out_of_range with message.
 */
inline void out_of_range(const char* function, int max, int index,
                         const char* msg1 = "", const char* msg2 = "") {
  std::ostringstream message;
  message << function << ": accessing element out of range. "
          << "index " << index << " out of range; "
          << "expecting index to be between " << stan::error_index::value
          << " and " << stan::error_index::value - 1 + max << msg1 << msg2;
  throw std::out_of_range(message.str());
}

}  // namespace math
}  // namespace stan
#endif
