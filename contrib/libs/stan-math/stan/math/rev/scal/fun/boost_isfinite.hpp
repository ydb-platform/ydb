#ifndef STAN_MATH_REV_SCAL_FUN_BOOST_ISFINITE_HPP
#define STAN_MATH_REV_SCAL_FUN_BOOST_ISFINITE_HPP

#include <boost/math/special_functions/fpclassify.hpp>
#include <stan/math/rev/core.hpp>

namespace boost {

namespace math {

/**
 * Checks if the given number has finite value.
 *
 * Return <code>true</code> if the specified variable's
 * value is finite.
 *
 * @param v Variable to test.
 * @return <code>true</code> if variable is finite.
 */
template <>
inline bool isfinite(const stan::math::var& v) {
  return (boost::math::isfinite)(v.val());
}

}  // namespace math
}  // namespace boost
#endif
