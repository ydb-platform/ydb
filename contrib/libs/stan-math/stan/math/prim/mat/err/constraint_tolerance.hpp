#ifndef STAN_MATH_PRIM_MAT_ERR_CONSTRAINT_TOLERANCE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CONSTRAINT_TOLERANCE_HPP

namespace stan {
namespace math {

/**
 * The tolerance for checking arithmetic bounds In rank and in
 * simplexes.  The default value is <code>1E-8</code>.
 */
const double CONSTRAINT_TOLERANCE = 1E-8;

}  // namespace math
}  // namespace stan
#endif
