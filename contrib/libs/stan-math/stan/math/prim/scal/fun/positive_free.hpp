#ifndef STAN_MATH_PRIM_SCAL_FUN_POSITIVE_FREE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_POSITIVE_FREE_HPP

#include <stan/math/prim/scal/err/check_positive.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the unconstrained value corresponding to the specified
 * positive-constrained value.
 *
 * <p>The transform is the inverse of the transform \f$f\f$ applied by
 * <code>positive_constrain(T)</code>, namely
 *
 * <p>\f$f^{-1}(x) = \log(x)\f$.
 *
 * <p>The input is validated using <code>check_positive()</code>.
 *
 * @param y Input scalar.
 * @return Unconstrained value that produces the input when constrained.
 * @tparam T Type of scalar.
 * @throw std::domain_error if the variable is negative
 */
template <typename T>
inline T positive_free(const T& y) {
  using std::log;
  check_positive("positive_free", "Positive variable", y);
  return log(y);
}

}  // namespace math
}  // namespace stan
#endif
