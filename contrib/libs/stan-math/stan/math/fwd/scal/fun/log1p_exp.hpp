#ifndef STAN_MATH_FWD_SCAL_FUN_LOG1P_EXP_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG1P_EXP_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> log1p_exp(const fvar<T>& x) {
  using std::exp;
  return fvar<T>(log1p_exp(x.val_), x.d_ / (1 + exp(-x.val_)));
}

}  // namespace math
}  // namespace stan
#endif
