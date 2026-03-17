#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_FINITE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_FINITE_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/domain_error_vec.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <boost/math/special_functions/fpclassify.hpp>

namespace stan {
namespace math {

namespace internal {
template <typename T_y, bool is_vec>
struct finite {
  static void check(const char* function, const char* name, const T_y& y) {
    if (!(boost::math::isfinite(value_of_rec(y))))
      domain_error(function, name, y, "is ", ", but must be finite!");
  }
};

template <typename T_y>
struct finite<T_y, true> {
  static void check(const char* function, const char* name, const T_y& y) {
    for (size_t n = 0; n < stan::length(y); n++) {
      if (!(boost::math::isfinite(value_of_rec(stan::get(y, n)))))
        domain_error_vec(function, name, y, n, "is ", ", but must be finite!");
    }
  }
};
}  // namespace internal

/**
 * Check if <code>y</code> is finite.
 * This function is vectorized and will check each element of
 * <code>y</code>.
 * @tparam T_y Type of y
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Variable to check
 * @throw <code>domain_error</code> if y is infinity, -infinity, or NaN
 */
template <typename T_y>
inline void check_finite(const char* function, const char* name, const T_y& y) {
  internal::finite<T_y, is_vector_like<T_y>::value>::check(function, name, y);
}
}  // namespace math
}  // namespace stan
#endif
