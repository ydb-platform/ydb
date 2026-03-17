#ifndef STAN_MATH_REV_CORE_OPERATOR_MINUS_EQUAL_HPP
#define STAN_MATH_REV_CORE_OPERATOR_MINUS_EQUAL_HPP

#include <stan/math/rev/core/var.hpp>
#include <stan/math/rev/core/operator_subtraction.hpp>

namespace stan {
namespace math {

inline var& var::operator-=(const var& b) {
  vi_ = new internal::subtract_vv_vari(vi_, b.vi_);
  return *this;
}

inline var& var::operator-=(double b) {
  if (b == 0.0)
    return *this;
  vi_ = new internal::subtract_vd_vari(vi_, b);
  return *this;
}

}  // namespace math
}  // namespace stan
#endif
