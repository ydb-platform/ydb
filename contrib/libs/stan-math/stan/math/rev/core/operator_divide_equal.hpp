#ifndef STAN_MATH_REV_CORE_OPERATOR_DIVIDE_EQUAL_HPP
#define STAN_MATH_REV_CORE_OPERATOR_DIVIDE_EQUAL_HPP

#include <stan/math/rev/core/var.hpp>
#include <stan/math/rev/core/operator_division.hpp>

namespace stan {
namespace math {

inline var& var::operator/=(const var& b) {
  vi_ = new internal::divide_vv_vari(vi_, b.vi_);
  return *this;
}

inline var& var::operator/=(double b) {
  if (b == 1.0)
    return *this;
  vi_ = new internal::divide_vd_vari(vi_, b);
  return *this;
}

}  // namespace math
}  // namespace stan
#endif
