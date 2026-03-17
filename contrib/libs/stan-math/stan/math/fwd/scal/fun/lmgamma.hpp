#ifndef STAN_MATH_FWD_SCAL_FUN_LMGAMMA_HPP
#define STAN_MATH_FWD_SCAL_FUN_LMGAMMA_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lmgamma.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<typename stan::return_type<T, int>::type> lmgamma(
    int x1, const fvar<T>& x2) {
  using std::log;
  T deriv = 0;
  for (int count = 1; count < x1 + 1; count++)
    deriv += x2.d_ * digamma(x2.val_ + (1.0 - count) / 2.0);
  return fvar<typename stan::return_type<T, int>::type>(lmgamma(x1, x2.val_),
                                                        deriv);
}
}  // namespace math
}  // namespace stan
#endif
