#ifndef STAN_MATH_PRIM_SCAL_FUN_ERF_HPP
#define STAN_MATH_PRIM_SCAL_FUN_ERF_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the error function of the specified value.
 *
 * \f[
 * \mbox{erf}(x) = \frac{2}{\sqrt{\pi}} \int_0^x e^{-t^2} dt
 * \f]
 *
 * @param[in] x Argument.
 * @return Error function of the argument.
 */
inline double erf(double x) { return std::erf(x); }

/**
 * Return the error function of the specified argument.  This
 * version is required to disambiguate <code>erf(int)</code>.
 *
 * @param[in] x Argument.
 * @return Error function of the argument.
 */
inline double erf(int x) { return std::erf(x); }

}  // namespace math
}  // namespace stan
#endif
