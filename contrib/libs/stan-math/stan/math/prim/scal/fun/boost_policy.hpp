#ifndef STAN_MATH_PRIM_SCAL_FUN_BOOST_POLICY_HPP
#define STAN_MATH_PRIM_SCAL_FUN_BOOST_POLICY_HPP

#include <boost/math/policies/policy.hpp>
#include <boost/math/policies/error_handling.hpp>

namespace stan {
namespace math {

/**
 * Boost policy that overrides the defaults to match the built-in
 * C++ standard library functions.
 *
 * The non-default behavior from Boost's built-ins are
 * (1) overflow errors return error numbers on error.
 * (2) pole errors return error numbers on error.
 */
typedef boost::math::policies::policy<
    boost::math::policies::overflow_error<
        boost::math::policies::errno_on_error>,
    boost::math::policies::pole_error<boost::math::policies::errno_on_error> >
    boost_policy_t;

}  // namespace math
}  // namespace stan
#endif
