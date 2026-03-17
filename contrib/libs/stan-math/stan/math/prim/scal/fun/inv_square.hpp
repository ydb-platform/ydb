#ifndef STAN_MATH_PRIM_SCAL_FUN_INV_SQUARE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_INV_SQUARE_HPP

namespace stan {
namespace math {

inline double inv_square(double x) { return 1.0 / (x * x); }
}  // namespace math
}  // namespace stan

#endif
