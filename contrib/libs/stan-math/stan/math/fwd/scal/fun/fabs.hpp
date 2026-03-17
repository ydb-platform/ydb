#ifndef STAN_MATH_FWD_SCAL_FUN_FABS_HPP
#define STAN_MATH_FWD_SCAL_FUN_FABS_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/likely.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> fabs(const fvar<T>& x) {
  using std::fabs;

  if (unlikely(is_nan(value_of(x.val_))))
    return fvar<T>(fabs(x.val_), NOT_A_NUMBER);
  else if (x.val_ > 0.0)
    return x;
  else if (x.val_ < 0.0)
    return fvar<T>(-x.val_, -x.d_);
  else
    return fvar<T>(0, 0);
}

}  // namespace math
}  // namespace stan
#endif
