#ifndef STAN_MATH_PRIM_SCAL_FUN_CHOOSE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_CHOOSE_HPP

#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <boost/math/special_functions/binomial.hpp>
#include <limits>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the binomial coefficient for the specified integer
 * arguments.
 *
 * The binomial coefficient, \f${n \choose k}\f$, read "n choose k", is
 * defined for \f$0 \leq k \leq n\f$ (otherwise return 0) by
 *
 * \f${n \choose k} = \frac{n!}{k! (n-k)!}\f$.
 *
 * @param n total number of objects
 * @param k number of objects chosen
 * @return n choose k or 0 iff k > n
 * @throw std::domain_error if either argument is negative or the
 * result will not fit in an int type
 */
inline int choose(int n, int k) {
  check_nonnegative("choose", "n", n);
  check_nonnegative("choose", "k", k);
  if (k > n)
    return 0;
  const double choices = boost::math::binomial_coefficient<double>(n, k);
  check_less_or_equal("choose", "n choose k", choices,
                      std::numeric_limits<int>::max());
  return static_cast<int>(std::round(choices));
}

}  // namespace math
}  // namespace stan
#endif
