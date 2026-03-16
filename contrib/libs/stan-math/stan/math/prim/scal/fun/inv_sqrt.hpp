#ifndef STAN_MATH_PRIM_SCAL_FUN_INV_SQRT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_INV_SQRT_HPP

#include <cmath>

namespace stan {
namespace math {

inline double inv_sqrt(double x) {
  using std::sqrt;
  return 1.0 / sqrt(x);
}

}  // namespace math
}  // namespace stan

#endif
