#ifndef STAN_MATH_PRIM_SCAL_FUN_LOGICAL_NEGATION_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOGICAL_NEGATION_HPP

namespace stan {
namespace math {

/**
 * The logical negation function which returns one if the input
 * is equal to zero and zero otherwise.
 *
 * @tparam T type of value
 * @param x value
 * @return 1 if value is zero and 0 otherwise
 */
template <typename T>
inline int logical_negation(const T& x) {
  return x == 0;
}

}  // namespace math
}  // namespace stan
#endif
