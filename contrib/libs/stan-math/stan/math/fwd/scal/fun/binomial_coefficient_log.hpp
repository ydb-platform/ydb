#ifndef STAN_MATH_FWD_SCAL_FUN_BINOMIAL_COEFFICIENT_LOG_HPP
#define STAN_MATH_FWD_SCAL_FUN_BINOMIAL_COEFFICIENT_LOG_HPP

#include <stan/math/fwd/core.hpp>

#include <boost/math/special_functions/digamma.hpp>
#include <stan/math/prim/scal/fun/binomial_coefficient_log.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> binomial_coefficient_log(const fvar<T>& x1, const fvar<T>& x2) {
  using boost::math::digamma;
  using std::log;
  const double cutoff = 1000;
  if ((x1.val_ < cutoff) || (x1.val_ - x2.val_ < cutoff)) {
    return fvar<T>(binomial_coefficient_log(x1.val_, x2.val_),
                   x1.d_ * digamma(x1.val_ + 1) - x2.d_ * digamma(x2.val_ + 1)
                       - (x1.d_ - x2.d_) * digamma(x1.val_ - x2.val_ + 1));
  } else {
    return fvar<T>(
        binomial_coefficient_log(x1.val_, x2.val_),
        x2.d_ * log(x1.val_ - x2.val_)
            + x2.val_ * (x1.d_ - x2.d_) / (x1.val_ - x2.val_)
            + x1.d_ * log(x1.val_ / (x1.val_ - x2.val_))
            + (x1.val_ + 0.5) / (x1.val_ / (x1.val_ - x2.val_))
                  * (x1.d_ * (x1.val_ - x2.val_) - (x1.d_ - x2.d_) * x1.val_)
                  / ((x1.val_ - x2.val_) * (x1.val_ - x2.val_))
            - x1.d_ / (12.0 * x1.val_ * x1.val_) - x2.d_
            + (x1.d_ - x2.d_)
                  / (12.0 * (x1.val_ - x2.val_) * (x1.val_ - x2.val_))
            - digamma(x2.val_ + 1) * x2.d_);
  }
}

template <typename T>
inline fvar<T> binomial_coefficient_log(const fvar<T>& x1, double x2) {
  using boost::math::digamma;
  using std::log;
  const double cutoff = 1000;
  if ((x1.val_ < cutoff) || (x1.val_ - x2 < cutoff)) {
    return fvar<T>(
        binomial_coefficient_log(x1.val_, x2),
        x1.d_ * digamma(x1.val_ + 1) - x1.d_ * digamma(x1.val_ - x2 + 1));
  } else {
    return fvar<T>(binomial_coefficient_log(x1.val_, x2),
                   x2 * x1.d_ / (x1.val_ - x2)
                       + x1.d_ * log(x1.val_ / (x1.val_ - x2))
                       + (x1.val_ + 0.5) / (x1.val_ / (x1.val_ - x2))
                             * (x1.d_ * (x1.val_ - x2) - x1.d_ * x1.val_)
                             / ((x1.val_ - x2) * (x1.val_ - x2))
                       - x1.d_ / (12.0 * x1.val_ * x1.val_)
                       + x1.d_ / (12.0 * (x1.val_ - x2) * (x1.val_ - x2)));
  }
}

template <typename T>
inline fvar<T> binomial_coefficient_log(double x1, const fvar<T>& x2) {
  using boost::math::digamma;
  using std::log;
  const double cutoff = 1000;
  if ((x1 < cutoff) || (x1 - x2.val_ < cutoff)) {
    return fvar<T>(
        binomial_coefficient_log(x1, x2.val_),
        -x2.d_ * digamma(x2.val_ + 1) - x2.d_ * digamma(x1 - x2.val_ + 1));
  } else {
    return fvar<T>(binomial_coefficient_log(x1, x2.val_),
                   x2.d_ * log(x1 - x2.val_) + x2.val_ * -x2.d_ / (x1 - x2.val_)
                       - x2.d_
                       - x2.d_ / (12.0 * (x1 - x2.val_) * (x1 - x2.val_))
                       + x2.d_ * (x1 + 0.5) / (x1 - x2.val_)
                       - digamma(x2.val_ + 1) * x2.d_);
  }
}
}  // namespace math
}  // namespace stan
#endif
