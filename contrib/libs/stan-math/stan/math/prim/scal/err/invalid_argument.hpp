#ifndef STAN_MATH_PRIM_SCAL_ERR_INVALID_ARGUMENT_HPP
#define STAN_MATH_PRIM_SCAL_ERR_INVALID_ARGUMENT_HPP

#include <typeinfo>
#include <sstream>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Throw an invalid_argument exception with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * invalid argument. This will allow us to change the behavior for all
 * functions at once.
 * The message is: "<function>: <name> <msg1><y><msg2>"
 * @tparam T Type of variable
 * @param function Name of the function
 * @param name Name of the variable
 * @param y Variable
 * @param msg1 Message to print before the variable
 * @param msg2 Message to print after the variable
 * @throw std::invalid_argument
 */
template <typename T>
inline void invalid_argument(const char* function, const char* name, const T& y,
                             const char* msg1, const char* msg2) {
  std::ostringstream message;
  message << function << ": " << name << " " << msg1 << y << msg2;
  throw std::invalid_argument(message.str());
}

/**
 * Throw an invalid_argument exception with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * invalid argument. This will allow us to change the behavior for all
 * functions at once. (We've already changed behavior mulitple times up
 * to Stan v2.5.0.)
 * The message is: "<function>: <name> <msg1><y>"
 * @tparam T Type of variable
 * @param function Name of the function
 * @param name Name of the variable
 * @param y Variable
 * @param msg1 Message to print before the variable
 * @throw std::invalid_argument
 */
template <typename T>
inline void invalid_argument(const char* function, const char* name, const T& y,
                             const char* msg1) {
  invalid_argument(function, name, y, msg1, "");
}

}  // namespace math
}  // namespace stan
#endif
