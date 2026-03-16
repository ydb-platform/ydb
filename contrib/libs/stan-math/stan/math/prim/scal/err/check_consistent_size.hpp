#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_CONSISTENT_SIZE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_CONSISTENT_SIZE_HPP

#include <stan/math/prim/scal/err/invalid_argument.hpp>
#include <stan/math/prim/scal/meta/size_of.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the dimension of x is consistent, which is defined to be
 * <code>expected_size</code> if x is a vector or 1 if x is not a vector.
 * @tparam T Type of value
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param x Variable to check for consistent size
 * @param expected_size Expected size if x is a vector
 * @throw <code>invalid_argument</code> if the size is inconsistent
 */
template <typename T>
inline void check_consistent_size(const char* function, const char* name,
                                  const T& x, size_t expected_size) {
  if (!is_vector<T>::value
      || (is_vector<T>::value && expected_size == stan::size_of(x)))
    return;

  std::stringstream msg;
  msg << ", expecting dimension = " << expected_size
      << "; a function was called with arguments of different "
      << "scalar, array, vector, or matrix types, and they were not "
      << "consistently sized;  all arguments must be scalars or "
      << "multidimensional values of the same shape.";
  std::string msg_str(msg.str());

  invalid_argument(function, name, stan::size_of(x),
                   "has dimension = ", msg_str.c_str());
}

}  // namespace math
}  // namespace stan
#endif
