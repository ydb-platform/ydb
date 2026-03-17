#ifndef STAN_MATH_REV_SCAL_FUN_BOOST_ISNORMAL_HPP
#define STAN_MATH_REV_SCAL_FUN_BOOST_ISNORMAL_HPP

#include <boost/math/special_functions/fpclassify.hpp>
#include <stan/math/rev/core.hpp>

namespace boost {
namespace math {

/**
 * Checks if the given number is normal.
 *
 * Return <code>true</code> if the specified variable
 * has a value that is normal.
 *
 * @param v Variable to test.
 * @return <code>true</code> if variable is normal.
 */
template <>
inline bool isnormal(const stan::math::var& v) {
  return (boost::math::isnormal)(v.val());
}

}  // namespace math
}  // namespace boost
#endif
