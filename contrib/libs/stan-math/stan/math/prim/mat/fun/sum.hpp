#ifndef STAN_MATH_PRIM_MAT_FUN_SUM_HPP
#define STAN_MATH_PRIM_MAT_FUN_SUM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/arr/fun/sum.hpp>

namespace stan {
namespace math {

/**
 * Returns the sum of the coefficients of the specified
 * Eigen Matrix, Array or expression.
 *
 * @tparam Derived Derived argument type.
 * @param v Specified argument.
 * @return Sum of coefficients of argument.
 */
template <typename Derived>
inline typename Eigen::DenseBase<Derived>::Scalar sum(
    const Eigen::DenseBase<Derived>& v) {
  return v.sum();
}

}  // namespace math
}  // namespace stan
#endif
