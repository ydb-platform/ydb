#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_GREATER_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_GREATER_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/domain_error_vec.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <functional>
#include <string>

namespace stan {
namespace math {

namespace internal {
template <typename T_y, typename T_low, bool is_vec>
struct greater {
  static void check(const char* function, const char* name, const T_y& y,
                    const T_low& low) {
    scalar_seq_view<T_low> low_vec(low);
    for (size_t n = 0; n < stan::length(low); n++) {
      if (!(y > low_vec[n])) {
        std::stringstream msg;
        msg << ", but must be greater than ";
        msg << low_vec[n];
        std::string msg_str(msg.str());
        domain_error(function, name, y, "is ", msg_str.c_str());
      }
    }
  }
};

template <typename T_y, typename T_low>
struct greater<T_y, T_low, true> {
  static void check(const char* function, const char* name, const T_y& y,
                    const T_low& low) {
    scalar_seq_view<T_low> low_vec(low);
    for (size_t n = 0; n < stan::length(y); n++) {
      if (!(stan::get(y, n) > low_vec[n])) {
        std::stringstream msg;
        msg << ", but must be greater than ";
        msg << low_vec[n];
        std::string msg_str(msg.str());
        domain_error_vec(function, name, y, n, "is ", msg_str.c_str());
      }
    }
  }
};
}  // namespace internal

/**
 * Check if <code>y</code> is strictly greater than <code>low</code>.
 * This function is vectorized and will check each element of
 * <code>y</code> against each element of <code>low</code>.
 * @tparam T_y Type of y
 * @tparam T_low Type of lower bound
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Variable to check
 * @param low Lower bound
 * @throw <code>domain_error</code> if y is not greater than low or
 *   if any element of y or low is NaN.
 */
template <typename T_y, typename T_low>
inline void check_greater(const char* function, const char* name, const T_y& y,
                          const T_low& low) {
  internal::greater<T_y, T_low, is_vector_like<T_y>::value>::check(
      function, name, y, low);
}
}  // namespace math
}  // namespace stan
#endif
