#ifndef STAN_MATH_REV_SCAL_FUN_TRIGAMMA_HPP
#define STAN_MATH_REV_SCAL_FUN_TRIGAMMA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/rev/scal/fun/floor.hpp>
#include <stan/math/rev/scal/fun/sin.hpp>
#include <stan/math/prim/scal/fun/trigamma.hpp>

namespace stan {
namespace math {

/**
 * Return the value of the trigamma function at the specified
 * argument (i.e., the second derivative of the log Gamma function
 * at the specified argument).
 *
 * @param u argument
 * @return trigamma function at argument
 */
inline var trigamma(const var& u) { return trigamma_impl(u); }

}  // namespace math
}  // namespace stan
#endif
