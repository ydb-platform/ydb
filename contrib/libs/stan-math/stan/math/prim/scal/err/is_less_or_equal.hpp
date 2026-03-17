#ifndef STAN_MATH_PRIM_SCAL_ERR_IS_LESS_OR_EQUAL_HPP
#define STAN_MATH_PRIM_SCAL_ERR_IS_LESS_OR_EQUAL_HPP

#include <stan/math/prim/scal/meta/get.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> if <code>y</code> is less or equal to
 * <code>high</code>.
 * This function is vectorized and will check each element of
 * <code>y</code> against each element of <code>high</code>.
 * @tparam T_y Type of y
 * @tparam T_high Type of upper bound
 * @param y Variable to check
 * @param high Upper bound
 * @return <code>true</code> if y is less than or equal
 *   to low and if and element of y or high is NaN
 */
template <typename T_y, typename T_high>
inline bool is_less_or_equal(const T_y& y, const T_high& high) {
  scalar_seq_view<T_high> high_vec(high);
  for (size_t n = 0; n < stan::length(high); n++) {
    if (!(stan::get(y, n) <= high_vec[n]))
      return false;
  }
  return true;
}

}  // namespace math
}  // namespace stan
#endif
