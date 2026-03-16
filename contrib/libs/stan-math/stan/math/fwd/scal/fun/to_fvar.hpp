#ifndef STAN_MATH_FWD_SCAL_FUN_TO_FVAR_HPP
#define STAN_MATH_FWD_SCAL_FUN_TO_FVAR_HPP

#include <stan/math/fwd/core.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> to_fvar(const T& x) {
  return fvar<T>(x);
}

template <typename T>
inline fvar<T> to_fvar(const fvar<T>& x) {
  return x;
}

}  // namespace math
}  // namespace stan
#endif
