#ifndef STAN_MATH_FWD_ARR_FUN_LOG_SUM_EXP_HPP
#define STAN_MATH_FWD_ARR_FUN_LOG_SUM_EXP_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/arr/fun/log_sum_exp.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T>
fvar<T> log_sum_exp(const std::vector<fvar<T> >& v) {
  using std::exp;
  std::vector<T> vals(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    vals[i] = v[i].val_;
  T deriv(0.0);
  T denominator(0.0);
  for (size_t i = 0; i < v.size(); ++i) {
    T exp_vi = exp(vals[i]);
    denominator += exp_vi;
    deriv += v[i].d_ * exp_vi;
  }
  return fvar<T>(log_sum_exp(vals), deriv / denominator);
}

}  // namespace math
}  // namespace stan
#endif
