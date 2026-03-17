#ifndef STAN_MATH_FWD_MAT_FUN_LOG_SUM_EXP_HPP
#define STAN_MATH_FWD_MAT_FUN_LOG_SUM_EXP_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/log_sum_exp.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/fwd/scal/fun/log.hpp>
#include <stan/math/fwd/scal/fun/exp.hpp>
#include <vector>

namespace stan {
namespace math {

// FIXME: cut-and-paste from fwd/log_sum_exp.hpp; should
// be able to generalize
template <typename T, int R, int C>
fvar<T> log_sum_exp(const Eigen::Matrix<fvar<T>, R, C>& v) {
  using std::exp;
  using std::log;

  Eigen::Matrix<T, 1, Eigen::Dynamic> vals(v.size());
  for (int i = 0; i < v.size(); ++i)
    vals[i] = v(i).val_;
  T deriv(0.0);
  T denominator(0.0);
  for (int i = 0; i < v.size(); ++i) {
    T exp_vi = exp(vals[i]);
    denominator += exp_vi;
    deriv += v(i).d_ * exp_vi;
  }
  return fvar<T>(log_sum_exp(vals), deriv / denominator);
}

}  // namespace math
}  // namespace stan
#endif
