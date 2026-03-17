#ifndef STAN_MATH_FWD_SCAL_FUN_MODIFIED_BESSEL_SECOND_KIND_HPP
#define STAN_MATH_FWD_SCAL_FUN_MODIFIED_BESSEL_SECOND_KIND_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/modified_bessel_second_kind.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> modified_bessel_second_kind(int v, const fvar<T>& z) {
  T modified_bessel_second_kind_z(modified_bessel_second_kind(v, z.val_));
  return fvar<T>(modified_bessel_second_kind_z,
                 -v * z.d_ * modified_bessel_second_kind_z / z.val_
                     - z.d_ * modified_bessel_second_kind(v - 1, z.val_));
}
}  // namespace math
}  // namespace stan
#endif
