#ifndef STAN_MATH_PRIM_SCAL_ERR_IS_SCAL_FINITE_HPP
#define STAN_MATH_PRIM_SCAL_ERR_IS_SCAL_FINITE_HPP

#include <stan/math/prim/scal/meta/get.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <boost/math/special_functions/fpclassify.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> if <code>y</code> is finite.
 * This function is vectorized and will check each element of
 * <code>y</code>.
 * @tparam T_y Type of y
 * @param y Variable to check
 * @throw <code>true</code> if y is not infinity, -infinity, or NaN
 */
template <typename T_y>
inline bool is_scal_finite(const T_y& y) {
  for (size_t n = 0; n < stan::length(y); ++n) {
    if (!(boost::math::isfinite(value_of_rec(stan::get(y, n)))))
      return false;
  }
  return true;
}

}  // namespace math
}  // namespace stan
#endif
