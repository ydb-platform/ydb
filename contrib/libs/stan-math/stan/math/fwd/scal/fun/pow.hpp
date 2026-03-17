#ifndef STAN_MATH_FWD_SCAL_FUN_POW_HPP
#define STAN_MATH_FWD_SCAL_FUN_POW_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/fwd/scal/fun/sqrt.hpp>
#include <stan/math/fwd/scal/fun/inv.hpp>
#include <stan/math/fwd/scal/fun/inv_sqrt.hpp>
#include <stan/math/fwd/scal/fun/inv_square.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> pow(const fvar<T>& x1, const fvar<T>& x2) {
  using std::log;
  using std::pow;
  T pow_x1_x2(pow(x1.val_, x2.val_));
  return fvar<T>(pow_x1_x2, (x2.d_ * log(x1.val_) + x2.val_ * x1.d_ / x1.val_)
                                * pow_x1_x2);
}

template <typename T>
inline fvar<T> pow(double x1, const fvar<T>& x2) {
  using std::log;
  using std::pow;
  T u = pow(x1, x2.val_);
  return fvar<T>(u, x2.d_ * log(x1) * u);
}

template <typename T>
inline fvar<T> pow(const fvar<T>& x1, double x2) {
  using std::pow;
  using std::sqrt;

  if (x2 == -2)
    return inv_square(x1);
  if (x2 == -1)
    return inv(x1);
  if (x2 == -0.5)
    return inv_sqrt(x1);
  if (x2 == 0.5)
    return sqrt(x1);
  if (x2 == 1.0)
    return x1;
  if (x2 == 2.0)
    return square(x1);

  return fvar<T>(pow(x1.val_, x2), x1.d_ * x2 * pow(x1.val_, x2 - 1));
}
}  // namespace math
}  // namespace stan
#endif
