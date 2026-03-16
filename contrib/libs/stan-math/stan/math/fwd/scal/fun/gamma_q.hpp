#ifndef STAN_MATH_FWD_SCAL_FUN_GAMMA_Q_HPP
#define STAN_MATH_FWD_SCAL_FUN_GAMMA_Q_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/tgamma.hpp>
#include <stan/math/prim/scal/fun/gamma_q.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> gamma_q(const fvar<T>& x1, const fvar<T>& x2) {
  using boost::math::digamma;
  using std::exp;
  using std::fabs;
  using std::log;
  using std::pow;

  T u = gamma_q(x1.val_, x2.val_);

  T S = 0;
  T s = 1;
  T l = log(x2.val_);
  T g = tgamma(x1.val_);
  T dig = digamma(x1.val_);

  int k = 0;
  T delta = s / (x1.val_ * x1.val_);

  while (fabs(delta) > 1e-6) {
    S += delta;
    ++k;
    s *= -x2.val_ / k;
    delta = s / ((k + x1.val_) * (k + x1.val_));
  }

  T der1 = (1.0 - u) * (dig - l) + exp(x1.val_ * l) * S / g;
  T der2 = -exp(-x2.val_) * pow(x2.val_, x1.val_ - 1.0) / g;

  return fvar<T>(u, x1.d_ * der1 + x2.d_ * der2);
}

template <typename T>
inline fvar<T> gamma_q(const fvar<T>& x1, double x2) {
  using boost::math::digamma;
  using std::exp;
  using std::fabs;
  using std::log;
  using std::pow;

  T u = gamma_q(x1.val_, x2);

  T S = 0;
  double s = 1;
  double l = log(x2);
  T g = tgamma(x1.val_);
  T dig = digamma(x1.val_);

  int k = 0;
  T delta = s / (x1.val_ * x1.val_);

  while (fabs(delta) > 1e-6) {
    S += delta;
    ++k;
    s *= -x2 / k;
    delta = s / ((k + x1.val_) * (k + x1.val_));
  }

  T der1 = (1.0 - u) * (dig - l) + exp(x1.val_ * l) * S / g;

  return fvar<T>(u, x1.d_ * der1);
}

template <typename T>
inline fvar<T> gamma_q(double x1, const fvar<T>& x2) {
  using std::exp;
  using std::pow;

  T u = gamma_q(x1, x2.val_);

  double g = tgamma(x1);

  T der2 = -exp(-x2.val_) * pow(x2.val_, x1 - 1.0) / g;

  return fvar<T>(u, x2.d_ * der2);
}
}  // namespace math
}  // namespace stan
#endif
