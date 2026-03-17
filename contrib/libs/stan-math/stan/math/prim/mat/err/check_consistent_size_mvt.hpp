#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_CONSISTENT_SIZE_MVT_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_CONSISTENT_SIZE_MVT_HPP

#include <stan/math/prim/mat/meta/length.hpp>
#include <stan/math/prim/mat/meta/length_mvt.hpp>
#include <stan/math/prim/mat/meta/is_vector.hpp>
#include <stan/math/prim/scal/err/invalid_argument.hpp>
#include <sstream>
#include <string>
#include <type_traits>

namespace stan {
namespace math {

/**
 * Check if the dimension of x is consistent, which is defined to be
 * <code>expected_size</code> if x is a vector of vectors or 1 if x is
 * a single vector.
 * @tparam T Type of value
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param x Variable to check for consistent size
 * @param expected_size Expected size if x is a vector
 * @throw <code>invalid_argument</code> if the size is inconsistent
 */
template <typename T>
inline void check_consistent_size_mvt(const char* function, const char* name,
                                      const T& x, size_t expected_size) {
  size_t size_x = 0;

  if (length(x) == 0) {
    size_x = 0;
    if (expected_size == 0)
      return;
  } else {
    size_t size_x = stan::length_mvt(x);
    bool x_contains_vectors = is_vector<
        typename std::remove_reference<decltype(x[0])>::type>::value;

    if (!x_contains_vectors)
      return;
    else if (expected_size == size_x)
      return;
  }

  std::stringstream msg;
  msg << ", expecting dimension = " << expected_size
      << "; a function was called with arguments of different "
      << "scalar, array, vector, or matrix types, and they were not "
      << "consistently sized;  all arguments must be scalars or "
      << "multidimensional values of the same shape.";
  std::string msg_str(msg.str());

  invalid_argument(function, name, size_x, "has dimension = ", msg_str.c_str());
}

}  // namespace math
}  // namespace stan
#endif
