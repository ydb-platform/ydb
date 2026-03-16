#ifndef STAN_MATH_PRIM_SCAL_FUN_LOGICAL_EQ_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOGICAL_EQ_HPP

namespace stan {
namespace math {

/**
 * Return 1 if the first argument is equal to the second.
 * Equivalent to <code>x1 == x2</code>.
 *
 * @tparam T1 Type of first argument.
 * @tparam T2 Type of second argument.
 * @param x1 First argument
 * @param x2 Second argument
 * @return <code>true</code> iff <code>x1 == x2</code>
 */
template <typename T1, typename T2>
inline int logical_eq(const T1 x1, const T2 x2) {
  return x1 == x2;
}

}  // namespace math
}  // namespace stan

#endif
