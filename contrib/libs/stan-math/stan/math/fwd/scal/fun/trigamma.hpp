#ifndef STAN_MATH_FWD_SCAL_FUN_TRIGAMMA_HPP
#define STAN_MATH_FWD_SCAL_FUN_TRIGAMMA_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/scal/fun/floor.hpp>
#include <stan/math/fwd/scal/fun/sin.hpp>
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
template <typename T>
inline fvar<T> trigamma(const fvar<T>& u) {
  return trigamma_impl(u);
}

}  // namespace math
}  // namespace stan
#endif
