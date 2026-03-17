#ifndef STAN_MATH_PRIM_SCAL_FUN_SIGN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SIGN_HPP

namespace stan {
namespace math {

// returns 1 if NaN is passed in.
template <typename T>
inline int sign(const T& z) {
  return (z == 0) ? 0 : z < 0 ? -1 : 1;
}
}  // namespace math
}  // namespace stan

#endif
