#ifndef STAN_MATH_REV_CORE_OPERATOR_UNARY_DECREMENT_HPP
#define STAN_MATH_REV_CORE_OPERATOR_UNARY_DECREMENT_HPP

#include <stan/math/rev/core/var.hpp>
#include <stan/math/rev/core/v_vari.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class decrement_vari : public op_v_vari {
 public:
  explicit decrement_vari(vari* avi) : op_v_vari(avi->val_ - 1.0, avi) {}
  void chain() {
    if (unlikely(is_nan(avi_->val_)))
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    else
      avi_->adj_ += adj_;
  }
};
}  // namespace internal

/**
 * Prefix decrement operator for variables (C++).
 *
 * Following C++, <code>(--a)</code> is defined to behave exactly as
 *
 * <code>a = a - 1.0)</code>
 *
 * does, but is faster and uses less memory.  In particular,
 * the result is an assignable lvalue.
 *
 * @param a Variable to decrement.
 * @return Reference the result of decrementing this input variable.
 */
inline var& operator--(var& a) {
  a.vi_ = new internal::decrement_vari(a.vi_);
  return a;
}

/**
 * Postfix decrement operator for variables (C++).
 *
 * Following C++, the expression <code>(a--)</code> is defined to
 * behave like the sequence of operations
 *
 * <code>var temp = a;  a = a - 1.0;  return temp;</code>
 *
 * @param a Variable to decrement.
 * @return Input variable.
 */
inline var operator--(var& a, int /*dummy*/) {
  var temp(a);
  a.vi_ = new internal::decrement_vari(a.vi_);
  return temp;
}

}  // namespace math
}  // namespace stan
#endif
