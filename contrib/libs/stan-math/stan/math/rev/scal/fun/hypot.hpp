#ifndef STAN_MATH_REV_SCAL_FUN_HYPOT_HPP
#define STAN_MATH_REV_SCAL_FUN_HYPOT_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/hypot.hpp>

namespace stan {
namespace math {

namespace internal {
class hypot_vv_vari : public op_vv_vari {
 public:
  hypot_vv_vari(vari* avi, vari* bvi)
      : op_vv_vari(hypot(avi->val_, bvi->val_), avi, bvi) {}
  void chain() {
    avi_->adj_ += adj_ * avi_->val_ / val_;
    bvi_->adj_ += adj_ * bvi_->val_ / val_;
  }
};

class hypot_vd_vari : public op_v_vari {
 public:
  hypot_vd_vari(vari* avi, double b) : op_v_vari(hypot(avi->val_, b), avi) {}
  void chain() { avi_->adj_ += adj_ * avi_->val_ / val_; }
};
}  // namespace internal

/**
 * Returns the length of the hypoteneuse of a right triangle
 * with sides of the specified lengths (C99).
 *
 * The partial derivatives are given by
 *
 * \f$\frac{\partial}{\partial x} \sqrt{x^2 + y^2} = \frac{x}{\sqrt{x^2 +
 * y^2}}\f$, and
 *
 * \f$\frac{\partial}{\partial y} \sqrt{x^2 + y^2} = \frac{y}{\sqrt{x^2 +
 * y^2}}\f$.
 *
 * @param[in] a Length of first side.
 * @param[in] b Length of second side.
 * @return Length of hypoteneuse.
 */
inline var hypot(const var& a, const var& b) {
  return var(new internal::hypot_vv_vari(a.vi_, b.vi_));
}

/**
 * Returns the length of the hypoteneuse of a right triangle
 * with sides of the specified lengths (C99).
 *
 * The derivative is
 *
 * \f$\frac{d}{d x} \sqrt{x^2 + c^2} = \frac{x}{\sqrt{x^2 + c^2}}\f$.
 *
 * @param[in] a Length of first side.
 * @param[in] b Length of second side.
 * @return Length of hypoteneuse.
 */
inline var hypot(const var& a, double b) {
  return var(new internal::hypot_vd_vari(a.vi_, b));
}

/**
 * Returns the length of the hypoteneuse of a right triangle
 * with sides of the specified lengths (C99).
 *
 * The derivative is
 *
 * \f$\frac{d}{d y} \sqrt{c^2 + y^2} = \frac{y}{\sqrt{c^2 + y^2}}\f$.
 *
   \f[
   \mbox{hypot}(x, y) =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < 0 \text{ or } y < 0 \\
     \sqrt{x^2+y^2} & \mbox{if } x, y\geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{hypot}(x, y)}{\partial x} =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < 0 \text{ or } y < 0 \\
     \frac{x}{\sqrt{x^2+y^2}} & \mbox{if } x, y\geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{hypot}(x, y)}{\partial y} =
   \begin{cases}
     \textrm{NaN} & \mbox{if } x < 0 \text{ or } y < 0 \\
     \frac{y}{\sqrt{x^2+y^2}} & \mbox{if } x, y\geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param[in] a Length of first side.
 * @param[in] b Length of second side.
 * @return Length of hypoteneuse.
 */
inline var hypot(double a, const var& b) {
  return var(new internal::hypot_vd_vari(b.vi_, a));
}

}  // namespace math
}  // namespace stan
#endif
