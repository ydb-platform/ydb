#ifndef STAN_MATH_PRIM_SCAL_FUN_SQUARE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SQUARE_HPP

namespace stan {
namespace math {

/**
 * Return the square of the specified argument.
 *
 * <p>\f$\mbox{square}(x) = x^2\f$.
 *
 * <p>The implementation of <code>square(x)</code> is just
 * <code>x * x</code>.  Given this, this method is mainly useful
 * in cases where <code>x</code> is not a simple primitive type,
 * particularly when it is an auto-dif type.
 *
 * @param x Input to square.
 * @return Square of input.
 */
inline double square(double x) { return x * x; }

}  // namespace math
}  // namespace stan

#endif
