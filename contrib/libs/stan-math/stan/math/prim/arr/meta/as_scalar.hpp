#ifndef STAN_MATH_PRIM_ARR_FUN_AS_SCALAR_HPP
#define STAN_MATH_PRIM_ARR_FUN_AS_SCALAR_HPP

#include <vector>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Converts input to a scalar. As this is not possible for vectors it always
 * throws. This is intended to never be called, only used in templated functions
 * in branches that will be optimized out - to prevent compiler from complaining
 * about expressions with incompatible types.
 * @param a Input expression
 * @throws runtime_error Always throws
 * @return Never returns
 */
template <typename T>
inline double as_scalar(const std::vector<T>& a) {
  throw std::runtime_error("A vector can not be used as a scalar!");
}

}  // namespace math
}  // namespace stan

#endif
