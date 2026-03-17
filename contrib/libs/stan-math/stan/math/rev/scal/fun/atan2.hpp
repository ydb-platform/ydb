#ifndef STAN_MATH_REV_SCAL_FUN_ATAN2_HPP
#define STAN_MATH_REV_SCAL_FUN_ATAN2_HPP

#include <stan/math/rev/core.hpp>
#include <cmath>
#include <valarray>

namespace stan {
namespace math {

namespace internal {
class atan2_vv_vari : public op_vv_vari {
 public:
  atan2_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(std::atan2(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    double a_sq_plus_b_sq
        = (avi_->val_ * avi_->val_) + (bvi_->val_ * bvi_->val_);
    avi_->adj_ += adj_ * bvi_->val_ / a_sq_plus_b_sq;
    bvi_->adj_ -= adj_ * avi_->val_ / a_sq_plus_b_sq;
  }
};

class atan2_vd_vari : public op_vd_vari {
 public:
  atan2_vd_vari(vari* avi, double b)
      : op_vd_vari(std::atan2(avi->val_, b), avi, b) {}
  void chain() {
    double a_sq_plus_b_sq = (avi_->val_ * avi_->val_) + (bd_ * bd_);
    avi_->adj_ += adj_ * bd_ / a_sq_plus_b_sq;
  }
};

class atan2_dv_vari : public op_dv_vari {
 public:
  atan2_dv_vari(double a, vari* bvi)
      : op_dv_vari(std::atan2(a, bvi->val_), a, bvi) {}
  void chain() {
    double a_sq_plus_b_sq = (ad_ * ad_) + (bvi_->val_ * bvi_->val_);
    bvi_->adj_ -= adj_ * ad_ / a_sq_plus_b_sq;
  }
};
}  // namespace internal

/**
 * Return the principal value of the arc tangent, in radians, of
 * the first variable divided by the second (cmath).
 *
 * The partial derivatives are defined by
 *
 * \f$ \frac{\partial}{\partial x} \arctan \frac{x}{y} = \frac{y}{x^2 + y^2}\f$,
 * and
 *
 * \f$ \frac{\partial}{\partial y} \arctan \frac{x}{y} = \frac{-x}{x^2 +
 * y^2}\f$.
 *
 * @param a Numerator variable.
 * @param b Denominator variable.
 * @return The arc tangent of the fraction, in radians.
 */
inline var atan2(const var& a, const var& b) {
  return var(new internal::atan2_vv_vari(a.vi_, b.vi_));
}

/**
 * Return the principal value of the arc tangent, in radians, of
 * the first variable divided by the second scalar (cmath).
 *
 * The derivative with respect to the variable is
 *
 * \f$ \frac{d}{d x} \arctan \frac{x}{c} = \frac{c}{x^2 + c^2}\f$.
 *
 * @param a Numerator variable.
 * @param b Denominator scalar.
 * @return The arc tangent of the fraction, in radians.
 */
inline var atan2(const var& a, double b) {
  return var(new internal::atan2_vd_vari(a.vi_, b));
}

/**
 * Return the principal value of the arc tangent, in radians, of
 * the first scalar divided by the second variable (cmath).
 *
 * The derivative with respect to the variable is
 *
 * \f$ \frac{\partial}{\partial y} \arctan \frac{c}{y} = \frac{-c}{c^2 +
 y^2}\f$.
 *
 *
   \f[
   \mbox{atan2}(x, y) =
   \begin{cases}
     \arctan\left(\frac{x}{y}\right) & \mbox{if } -\infty\leq x \leq \infty,
 -\infty\leq y \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or
 } y = \textrm{NaN} \end{cases} \f]

   \f[
   \frac{\partial\, \mbox{atan2}(x, y)}{\partial x} =
   \begin{cases}
     \frac{y}{x^2+y^2} & \mbox{if } -\infty\leq x\leq \infty, -\infty\leq y \leq
 \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{atan2}(x, y)}{\partial y} =
   \begin{cases}
     -\frac{x}{x^2+y^2} & \mbox{if } -\infty\leq x\leq \infty, -\infty\leq y
 \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y =
 \textrm{NaN} \end{cases} \f]
 *
 * @param a Numerator scalar.
 * @param b Denominator variable.
 * @return The arc tangent of the fraction, in radians.
 */
inline var atan2(double a, const var& b) {
  return var(new internal::atan2_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
