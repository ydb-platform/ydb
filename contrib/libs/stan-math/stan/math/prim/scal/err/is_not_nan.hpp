#ifndef STAN_MATH_PRIM_SCAL_ERR_IS_NOT_NAN_HPP
#define STAN_MATH_PRIM_SCAL_ERR_IS_NOT_NAN_HPP

#include <stan/math/prim/scal/meta/get.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> if <code>y</code> is not <code>NaN</code>.
 * This function is vectorized and will check each element of
 * <code>y</code>. If no element is <code>NaN</code>, this
 * function will return <code>true</code>.
 * @tparam T_y Type of y
 * @param y Variable to check
 * @return <code>true</code> if no element of y is NaN
 */
template <typename T_y>
inline bool is_not_nan(const T_y& y) {
  for (size_t n = 0; n < stan::length(y); ++n) {
    if (is_nan(value_of_rec(stan::get(y, n))))
      return false;
  }
  return true;
}

}  // namespace math
}  // namespace stan
#endif
