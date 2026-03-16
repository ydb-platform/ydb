#ifndef STAN_MATH_PRIM_SCAL_ERR_SYSTEM_ERROR_HPP
#define STAN_MATH_PRIM_SCAL_ERR_SYSTEM_ERROR_HPP

#include <typeinfo>
#include <sstream>
#include <stdexcept>
#include <system_error>
namespace stan {
namespace math {

/**
 * Throw a system error with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * system errors. This will allow us to change the behavior for all
 * functions at once.
 * The message is: "<function>: <name> <msg1><y><msg2>"
 * @param[in] function Name of the function.
 * @param[in] name Name of the variable.
 * @param[in] y Error code.
 * @param[in] msg1 Message to print before the variable.
 * @param[in] msg2 Message to print after the variable.
 * @throw std::system_error Always.
 */
inline void system_error(const char* function, const char* name, const int& y,
                         const char* msg1, const char* msg2) {
  std::ostringstream message;
  // hack to remove -Waddress, -Wnonnull-compare warnings from GCC 6
  message << function << ": " << name << " " << msg1 << msg2;
  throw std::system_error(y, std::generic_category(), message.str());
}

/**
 * Throw a system error with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * system errors. This will allow us to change the behavior for all
 * functions at once.
 * The message is: * "<function>: <name> <msg1><y>"
 * @param[in] function Name of the function.
 * @param[in] name Name of the variable.
 * @param[in] y Error code.
 * @param[in] msg1 Message to print before the variable.
 * @throw std::system_error Always.
 */
inline void system_error(const char* function, const char* name, const int& y,
                         const char* msg1) {
  system_error(function, name, y, msg1, "");
}

}  // namespace math
}  // namespace stan
#endif
