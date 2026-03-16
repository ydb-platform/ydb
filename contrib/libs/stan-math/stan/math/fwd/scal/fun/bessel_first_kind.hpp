#ifndef STAN_MATH_FWD_SCAL_FUN_BESSEL_FIRST_KIND_HPP
#define STAN_MATH_FWD_SCAL_FUN_BESSEL_FIRST_KIND_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/bessel_first_kind.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> bessel_first_kind(int v, const fvar<T>& z) {
  T bessel_first_kind_z(bessel_first_kind(v, z.val_));
  return fvar<T>(bessel_first_kind_z,
                 v * z.d_ * bessel_first_kind_z / z.val_
                     - z.d_ * bessel_first_kind(v + 1, z.val_));
}
}  // namespace math
}  // namespace stan
#endif
