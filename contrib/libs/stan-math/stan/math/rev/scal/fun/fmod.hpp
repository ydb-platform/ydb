#ifndef STAN_MATH_REV_SCAL_FUN_FMOD_HPP
#define STAN_MATH_REV_SCAL_FUN_FMOD_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class fmod_vv_vari : public op_vv_vari {
 public:
  fmod_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(std::fmod(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    if (unlikely(is_nan(avi_->val_) || is_nan(bvi_->val_))) {
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
      bvi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    } else {
      avi_->adj_ += adj_;
      bvi_->adj_ -= adj_ * static_cast<int>(avi_->val_ / bvi_->val_);
    }
  }
};

class fmod_vd_vari : public op_vd_vari {
 public:
  fmod_vd_vari(vari* avi, double b)
      : op_vd_vari(std::fmod(avi->val_, b), avi, b) {}
  void chain() {
    if (unlikely(is_nan(avi_->val_) || is_nan(bd_)))
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    else
      avi_->adj_ += adj_;
  }
};

class fmod_dv_vari : public op_dv_vari {
 public:
  fmod_dv_vari(double a, vari* bvi)
      : op_dv_vari(std::fmod(a, bvi->val_), a, bvi) {}
  void chain() {
    if (unlikely(is_nan(bvi_->val_) || is_nan(ad_))) {
      bvi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    } else {
      int d = static_cast<int>(ad_ / bvi_->val_);
      bvi_->adj_ -= adj_ * d;
    }
  }
};
}  // namespace internal

/**
 * Return the floating point remainder after dividing the
 * first variable by the second (cmath).
 *
 * The partial derivatives with respect to the variables are defined
 * everywhere but where \f$x = y\f$, but we set these to match other values,
 * with
 *
 * \f$\frac{\partial}{\partial x} \mbox{fmod}(x, y) = 1\f$, and
 *
 * \f$\frac{\partial}{\partial y} \mbox{fmod}(x, y) = -\lfloor \frac{x}{y}
 \rfloor\f$.
 *
 *
   \f[
   \mbox{fmod}(x, y) =
   \begin{cases}
     x - \lfloor \frac{x}{y}\rfloor y & \mbox{if } -\infty\leq x, y \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{fmod}(x, y)}{\partial x} =
   \begin{cases}
     1 & \mbox{if } -\infty\leq x, y\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{fmod}(x, y)}{\partial y} =
   \begin{cases}
     -\lfloor \frac{x}{y}\rfloor & \mbox{if } -\infty\leq x, y\leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a First variable.
 * @param b Second variable.
 * @return Floating pointer remainder of dividing the first variable
 * by the second.
 */
inline var fmod(const var& a, const var& b) {
  return var(new internal::fmod_vv_vari(a.vi_, b.vi_));
}

/**
 * Return the floating point remainder after dividing the
 * the first variable by the second scalar (cmath).
 *
 * The derivative with respect to the variable is
 *
 * \f$\frac{d}{d x} \mbox{fmod}(x, c) = \frac{1}{c}\f$.
 *
 * @param a First variable.
 * @param b Second scalar.
 * @return Floating pointer remainder of dividing the first variable by
 * the second scalar.
 */
inline var fmod(const var& a, double b) {
  return var(new internal::fmod_vd_vari(a.vi_, b));
}

/**
 * Return the floating point remainder after dividing the
 * first scalar by the second variable (cmath).
 *
 * The derivative with respect to the variable is
 *
 * \f$\frac{d}{d y} \mbox{fmod}(c, y) = -\lfloor \frac{c}{y} \rfloor\f$.
 *
 * @param a First scalar.
 * @param b Second variable.
 * @return Floating pointer remainder of dividing first scalar by
 * the second variable.
 */
inline var fmod(double a, const var& b) {
  return var(new internal::fmod_dv_vari(a, b.vi_));
}

}  // namespace math
}  // namespace stan
#endif
