#ifndef STAN_MATH_FWD_SCAL_FUN_LOG_SUM_EXP_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG_SUM_EXP_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/log_sum_exp.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> log_sum_exp(const fvar<T>& x1, const fvar<T>& x2) {
  using std::exp;
  return fvar<T>(log_sum_exp(x1.val_, x2.val_),
                 x1.d_ / (1 + exp(x2.val_ - x1.val_))
                     + x2.d_ / (exp(x1.val_ - x2.val_) + 1));
}

template <typename T>
inline fvar<T> log_sum_exp(double x1, const fvar<T>& x2) {
  using std::exp;
  return fvar<T>(log_sum_exp(x1, x2.val_), x2.d_ / (exp(x1 - x2.val_) + 1));
}

template <typename T>
inline fvar<T> log_sum_exp(const fvar<T>& x1, double x2) {
  using std::exp;
  return fvar<T>(log_sum_exp(x1.val_, x2), x1.d_ / (1 + exp(x2 - x1.val_)));
}

}  // namespace math
}  // namespace stan
#endif
