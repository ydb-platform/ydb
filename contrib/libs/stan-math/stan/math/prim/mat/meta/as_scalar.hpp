#ifndef STAN_MATH_PRIM_MAT_FUN_AS_SCALAR_HPP
#define STAN_MATH_PRIM_MAT_FUN_AS_SCALAR_HPP

#include <Eigen/Dense>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Converts input to a scalar. As this is not possible for matrices, arrays or
 * Eigen expressions it always throws. This is intended to never be called, only
 * used in templated functions in branches that will be optimized out - to
 * prevent compiler from complaining about expressions with incompatible types.
 * @tparam Derived Type of input Eigen expression.
 * @param a Input expression
 * @throws runtime_error Always throws
 * @return Never returns
 */
template <typename Derived>
inline double as_scalar(const Eigen::DenseBase<Derived>& a) {
  throw std::runtime_error("A matrix can not be used as a scalar!");
}

}  // namespace math
}  // namespace stan

#endif
