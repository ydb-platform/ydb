#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_POSITIVE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_POSITIVE_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/domain_error_vec.hpp>
#include <stan/math/prim/scal/err/invalid_argument.hpp>
#include <stan/math/prim/scal/meta/value_type.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/get.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <type_traits>
#include <string>

namespace stan {
namespace math {

namespace {

template <typename T_y, bool is_vec>
struct positive {
  static void check(const char* function, const char* name, const T_y& y) {
    // have to use not is_unsigned. is_signed will be false
    // floating point types that have no unsigned versions.
    if (!std::is_unsigned<T_y>::value && !(y > 0))
      domain_error(function, name, y, "is ", ", but must be > 0!");
  }
};

template <typename T_y>
struct positive<T_y, true> {
  static void check(const char* function, const char* name, const T_y& y) {
    using stan::length;
    for (size_t n = 0; n < length(y); n++) {
      if (!std::is_unsigned<typename value_type<T_y>::type>::value
          && !(stan::get(y, n) > 0))
        domain_error_vec(function, name, y, n, "is ", ", but must be > 0!");
    }
  }
};

}  // namespace

/**
 * Check if <code>y</code> is positive.
 * This function is vectorized and will check each element of
 * <code>y</code>.
 * @tparam T_y Type of y
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Variable to check
 * @throw <code>domain_error</code> if y is negative or zero or
 *   if any element of y is NaN
 */
template <typename T_y>
inline void check_positive(const char* function, const char* name,
                           const T_y& y) {
  positive<T_y, is_vector_like<T_y>::value>::check(function, name, y);
}

/**
 * Check if <code>size</code> is positive.
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param expr Expression for the dimension size (for error messages)
 * @param size Size value to check
 * @throw <code>std::invalid_argument</code> if <code>size</code> is
 *   zero or negative.
 */
inline void check_positive(const char* function, const char* name,
                           const char* expr, int size) {
  if (size <= 0) {
    std::stringstream msg;
    msg << "; dimension size expression = " << expr;
    std::string msg_str(msg.str());
    invalid_argument(function, name, size, "must have a positive size, but is ",
                     msg_str.c_str());
  }
}

}  // namespace math
}  // namespace stan
#endif
