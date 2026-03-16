#ifndef STAN_MATH_REV_CORE_OPERATOR_UNARY_INCREMENT_HPP
#define STAN_MATH_REV_CORE_OPERATOR_UNARY_INCREMENT_HPP

#include <stan/math/rev/core/var.hpp>
#include <stan/math/rev/core/v_vari.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <limits>

namespace stan {
namespace math {

namespace internal {
class increment_vari : public op_v_vari {
 public:
  explicit increment_vari(vari* avi) : op_v_vari(avi->val_ + 1.0, avi) {}
  void chain() {
    if (unlikely(is_nan(avi_->val_)))
      avi_->adj_ = std::numeric_limits<double>::quiet_NaN();
    else
      avi_->adj_ += adj_;
  }
};
}  // namespace internal

/**
 * Prefix increment operator for variables (C++).  Following C++,
 * (++a) is defined to behave exactly as (a = a + 1.0) does,
 * but is faster and uses less memory.  In particular, the
 * result is an assignable lvalue.
 *
 * @param a Variable to increment.
 * @return Reference the result of incrementing this input variable.
 */
inline var& operator++(var& a) {
  a.vi_ = new internal::increment_vari(a.vi_);
  return a;
}

/**
 * Postfix increment operator for variables (C++).
 *
 * Following C++, the expression <code>(a++)</code> is defined to behave like
 * the sequence of operations
 *
 * <code>var temp = a;  a = a + 1.0;  return temp;</code>
 *
 * @param a Variable to increment.
 * @return Input variable.
 */
inline var operator++(var& a, int /*dummy*/) {
  var temp(a);
  a.vi_ = new internal::increment_vari(a.vi_);
  return temp;
}

}  // namespace math
}  // namespace stan
#endif
