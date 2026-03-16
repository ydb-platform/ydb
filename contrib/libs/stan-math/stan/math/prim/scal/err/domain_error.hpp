#ifndef STAN_MATH_PRIM_SCAL_ERR_DOMAIN_ERROR_HPP
#define STAN_MATH_PRIM_SCAL_ERR_DOMAIN_ERROR_HPP

#include <typeinfo>
#include <sstream>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Throw a domain error with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * domain errors. This will allow us to change the behavior for all
 * functions at once.
 * The message is: "<function>: <name> <msg1><y><msg2>"
 * @tparam T Type of variable.
 * @param[in] function Name of the function.
 * @param[in] name Name of the variable.
 * @param[in] y Variable.
 * @param[in] msg1 Message to print before the variable.
 * @param[in] msg2 Message to print after the variable.
 * @throw std::domain_error Always.
 */
template <typename T>
inline void domain_error(const char* function, const char* name, const T& y,
                         const char* msg1, const char* msg2) {
  std::ostringstream message;
  // hack to remove -Waddress, -Wnonnull-compare warnings from GCC 6
  const T* y_ptr = &y;
  message << function << ": " << name << " " << msg1 << (*y_ptr) << msg2;
  throw std::domain_error(message.str());
}

/**
 * Throw a domain error with a consistently formatted message.
 * This is an abstraction for all Stan functions to use when throwing
 * domain errors. This will allow us to change the behavior for all
 * functions at once.
 * The message is: * "<function>: <name> <msg1><y>"
 * @tparam T Type of variable.
 * @param[in] function Name of the function.
 * @param[in] name Name of the variable.
 * @param[in] y Variable.
 * @param[in] msg1 Message to print before the variable.
 * @throw std::domain_error Always.
 */
template <typename T>
inline void domain_error(const char* function, const char* name, const T& y,
                         const char* msg1) {
  domain_error(function, name, y, msg1, "");
}

}  // namespace math
}  // namespace stan
#endif
