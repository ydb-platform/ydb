#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_BOUNDED_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_BOUNDED_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/domain_error_vec.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <string>

namespace stan {
namespace math {

namespace internal {

// implemented using structs because there is no partial specialization
// for templated functions
// default implementation works for scalar T_y. T_low and T_high can
// be either scalar or vector
// throws if y, low, or high is nan
template <typename T_y, typename T_low, typename T_high, bool y_is_vec>
struct bounded {
  static void check(const char* function, const char* name, const T_y& y,
                    const T_low& low, const T_high& high) {
    scalar_seq_view<T_low> low_vec(low);
    scalar_seq_view<T_high> high_vec(high);
    for (size_t n = 0; n < stan::max_size(low, high); n++) {
      if (!(low_vec[n] <= y && y <= high_vec[n])) {
        std::stringstream msg;
        msg << ", but must be in the interval ";
        msg << "[" << low_vec[n] << ", " << high_vec[n] << "]";
        std::string msg_str(msg.str());
        domain_error(function, name, y, "is ", msg_str.c_str());
      }
    }
  }
};

template <typename T_y, typename T_low, typename T_high>
struct bounded<T_y, T_low, T_high, true> {
  static void check(const char* function, const char* name, const T_y& y,
                    const T_low& low, const T_high& high) {
    scalar_seq_view<T_low> low_vec(low);
    scalar_seq_view<T_high> high_vec(high);
    for (size_t n = 0; n < stan::length(y); n++) {
      if (!(low_vec[n] <= stan::get(y, n) && stan::get(y, n) <= high_vec[n])) {
        std::stringstream msg;
        msg << ", but must be in the interval ";
        msg << "[" << low_vec[n] << ", " << high_vec[n] << "]";
        std::string msg_str(msg.str());
        domain_error_vec(function, name, y, n, "is ", msg_str.c_str());
      }
    }
  }
};
}  // namespace internal

/**
 * Check if the value is between the low and high values, inclusively.
 * @tparam T_y Type of value
 * @tparam T_low Type of low value
 * @tparam T_high Type of high value
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Value to check
 * @param low Low bound
 * @param high High bound
 * @throw <code>std::domain_error</code> otherwise. This also throws
 *   if any of the arguments are NaN.
 */
template <typename T_y, typename T_low, typename T_high>
inline void check_bounded(const char* function, const char* name, const T_y& y,
                          const T_low& low, const T_high& high) {
  internal::bounded<T_y, T_low, T_high, is_vector_like<T_y>::value>::check(
      function, name, y, low, high);
}

}  // namespace math
}  // namespace stan
#endif
