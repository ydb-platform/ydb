#ifndef STAN_MATH_PRIM_SCAL_FUN_PROB_FREE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PROB_FREE_HPP

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/fun/logit.hpp>

namespace stan {
namespace math {

/**
 * Return the free scalar that when transformed to a probability
 * produces the specified scalar.
 *
 * <p>The function that reverses the constraining transform
 * specified in <code>prob_constrain(T)</code> is the logit
 * function,
 *
 * <p>\f$f^{-1}(y) = \mbox{logit}(y) = \frac{1 - y}{y}\f$.
 *
 * @tparam T type of constrained value
 * @param y constrained value
 * @return corresponding unconstrained value
 * @throw std::domain_error if y is not in (0, 1)
 */
template <typename T>
inline T prob_free(const T& y) {
  check_bounded<T, double, double>("prob_free", "Probability variable", y, 0,
                                   1);
  return logit(y);
}

}  // namespace math
}  // namespace stan
#endif
