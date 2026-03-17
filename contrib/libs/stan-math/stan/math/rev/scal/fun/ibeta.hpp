#ifndef STAN_MATH_REV_SCAL_FUN_IBETA_HPP
#define STAN_MATH_REV_SCAL_FUN_IBETA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/ibeta.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/tgamma.hpp>

namespace stan {
namespace math {

namespace internal {
/**
 * Calculates the generalized hypergeometric 3F2(a, a, b; a + 1, a + 1; z).
 *
 * Handles negative values of b properly.
 */
inline double ibeta_hypergeometric_helper(double a, double b, double z,
                                          double precision = 1e-8,
                                          double max_steps = 1000) {
  double val = 0;
  double diff = 1;
  double k = 0;
  double a_2 = a * a;
  double bprod = 1;
  while (std::abs(diff) > precision && ++k < max_steps) {
    val += diff;
    bprod *= b + k - 1.0;
    diff = a_2 * std::pow(a + k, -2) * bprod * std::pow(z, k) / tgamma(k + 1);
  }
  return val;
}

class ibeta_vvv_vari : public op_vvv_vari {
 public:
  ibeta_vvv_vari(vari* avi, vari* bvi, vari* xvi)
      : op_vvv_vari(ibeta(avi->val_, bvi->val_, xvi->val_), avi, bvi, xvi) {}
  void chain() {
    double a = avi_->val_;
    double b = bvi_->val_;
    double c = cvi_->val_;

    using boost::math::constants::pi;
    using std::log;
    using std::pow;
    using std::sin;
    avi_->adj_ += adj_ * (log(c) - digamma(a) + digamma(a + b)) * val_
                  - tgamma(a) * tgamma(a + b) / tgamma(b) * pow(c, a)
                        / tgamma(1 + a) / tgamma(1 + a)
                        * ibeta_hypergeometric_helper(a, 1 - b, c);
    bvi_->adj_ += adj_
                  * (tgamma(b) * tgamma(a + b) / tgamma(a) * pow(1 - c, b)
                         * ibeta_hypergeometric_helper(b, 1 - a, 1 - c)
                         / tgamma(b + 1) / tgamma(b + 1)
                     + ibeta(b, a, 1 - c)
                           * (digamma(b) - digamma(a + b) - log(1 - c)));
    cvi_->adj_ += adj_ * boost::math::ibeta_derivative(a, b, c);
  }
};
class ibeta_vvd_vari : public op_vvd_vari {
 public:
  ibeta_vvd_vari(vari* avi, vari* bvi, double x)
      : op_vvd_vari(ibeta(avi->val_, bvi->val_, x), avi, bvi, x) {}
  void chain() {
    double a = avi_->val_;
    double b = bvi_->val_;
    double c = cd_;

    using boost::math::constants::pi;
    using std::log;
    using std::pow;
    using std::sin;
    avi_->adj_ += adj_ * (log(c) - digamma(a) + digamma(a + b)) * val_
                  - tgamma(a) * tgamma(a + b) / tgamma(b) * pow(c, a)
                        / tgamma(1 + a) / tgamma(1 + a)
                        * ibeta_hypergeometric_helper(a, 1 - b, c);
    bvi_->adj_ += adj_
                  * (tgamma(b) * tgamma(a + b) / tgamma(a) * pow(1 - c, b)
                         * ibeta_hypergeometric_helper(b, 1 - a, 1 - c)
                         / tgamma(b + 1) / tgamma(b + 1)
                     + ibeta(b, a, 1 - c)
                           * (digamma(b) - digamma(a + b) - log(1 - c)));
  }
};
class ibeta_vdv_vari : public op_vdv_vari {
 public:
  ibeta_vdv_vari(vari* avi, double b, vari* xvi)
      : op_vdv_vari(ibeta(avi->val_, b, xvi->val_), avi, b, xvi) {}
  void chain() {
    double a = avi_->val_;
    double b = bd_;
    double c = cvi_->val_;

    using boost::math::constants::pi;
    using boost::math::digamma;
    using boost::math::ibeta;
    using std::log;
    using std::pow;
    using std::sin;
    avi_->adj_ += adj_ * (log(c) - digamma(a) + digamma(a + b)) * val_
                  - tgamma(a) * tgamma(a + b) / tgamma(b) * pow(c, a)
                        / tgamma(1 + a) / tgamma(1 + a)
                        * ibeta_hypergeometric_helper(a, 1 - b, c);
    cvi_->adj_ += adj_ * boost::math::ibeta_derivative(a, b, c);
  }
};
class ibeta_vdd_vari : public op_vdd_vari {
 public:
  ibeta_vdd_vari(vari* avi, double b, double x)
      : op_vdd_vari(ibeta(avi->val_, b, x), avi, b, x) {}
  void chain() {
    double a = avi_->val_;
    double b = bd_;
    double c = cd_;

    using boost::math::constants::pi;
    using boost::math::digamma;
    using boost::math::ibeta;
    using std::log;
    using std::pow;
    using std::sin;
    avi_->adj_ += adj_ * (log(c) - digamma(a) + digamma(a + b)) * val_
                  - tgamma(a) * tgamma(a + b) / tgamma(b) * pow(c, a)
                        / tgamma(1 + a) / tgamma(1 + a)
                        * ibeta_hypergeometric_helper(a, 1 - b, c);
  }
};
class ibeta_dvv_vari : public op_dvv_vari {
 public:
  ibeta_dvv_vari(double a, vari* bvi, vari* xvi)
      : op_dvv_vari(ibeta(a, bvi->val_, xvi->val_), a, bvi, xvi) {}
  void chain() {
    double a = ad_;
    double b = bvi_->val_;
    double c = cvi_->val_;

    using boost::math::constants::pi;
    using boost::math::digamma;
    using boost::math::ibeta;
    using std::log;
    using std::pow;
    using std::sin;
    bvi_->adj_ += adj_
                  * (tgamma(b) * tgamma(a + b) / tgamma(a) * pow(1 - c, b)
                         * ibeta_hypergeometric_helper(b, 1 - a, 1 - c)
                         / tgamma(b + 1) / tgamma(b + 1)
                     + ibeta(b, a, 1 - c)
                           * (digamma(b) - digamma(a + b) - log(1 - c)));
    cvi_->adj_ += adj_ * boost::math::ibeta_derivative(a, b, c);
  }
};
class ibeta_dvd_vari : public op_dvd_vari {
 public:
  ibeta_dvd_vari(double a, vari* bvi, double x)
      : op_dvd_vari(ibeta(a, bvi->val_, x), a, bvi, x) {}
  void chain() {
    double a = ad_;
    double b = bvi_->val_;
    double c = cd_;

    using boost::math::constants::pi;
    using boost::math::digamma;
    using boost::math::ibeta;
    using std::log;
    using std::pow;
    using std::sin;
    bvi_->adj_ += adj_
                  * (tgamma(b) * tgamma(a + b) / tgamma(a) * pow(1 - c, b)
                         * ibeta_hypergeometric_helper(b, 1 - a, 1 - c)
                         / tgamma(b + 1) / tgamma(b + 1)
                     + ibeta(b, a, 1 - c)
                           * (digamma(b) - digamma(a + b) - log(1 - c)));
  }
};
class ibeta_ddv_vari : public op_ddv_vari {
 public:
  ibeta_ddv_vari(double a, double b, vari* xvi)
      : op_ddv_vari(ibeta(a, b, xvi->val_), a, b, xvi) {}
  void chain() {
    double a = ad_;
    double b = bd_;
    double c = cvi_->val_;

    cvi_->adj_ += adj_ * boost::math::ibeta_derivative(a, b, c);
  }
};
}  // namespace internal

/**
 * The normalized incomplete beta function of a, b, and x.
 *
 * Used to compute the cumulative density function for the beta
 * distribution.
 *
 * Partial derivatives are those specified by wolfram alpha.
 * The values were checked using both finite differences and
 * by independent code for calculating the derivatives found
 * in JSS (paper by Boik and Robison-Cox).
 *
 * @param a Shape parameter.
 * @param b Shape parameter.
 * @param x Random variate.
 *
 * @return The normalized incomplete beta function.
 * @throws if any argument is NaN.
 */
inline var ibeta(const var& a, const var& b, const var& x) {
  return var(new internal::ibeta_vvv_vari(a.vi_, b.vi_, x.vi_));
}

}  // namespace math
}  // namespace stan
#endif
