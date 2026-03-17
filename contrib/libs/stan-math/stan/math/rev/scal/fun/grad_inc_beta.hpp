#ifndef STAN_MATH_REV_SCAL_FUN_GRAD_INC_BETA_HPP
#define STAN_MATH_REV_SCAL_FUN_GRAD_INC_BETA_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/grad_2F1.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/rev/scal/fun/exp.hpp>
#include <stan/math/rev/scal/fun/fabs.hpp>
#include <stan/math/rev/scal/fun/floor.hpp>
#include <stan/math/rev/scal/fun/value_of_rec.hpp>
#include <stan/math/rev/scal/fun/inc_beta.hpp>
#include <stan/math/rev/scal/fun/is_nan.hpp>
#include <stan/math/rev/scal/fun/lgamma.hpp>
#include <stan/math/rev/scal/fun/log.hpp>
#include <stan/math/rev/scal/fun/log1m.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Gradient of the incomplete beta function beta(a, b, z) with
 * respect to the first two arguments.
 *
 * Uses the equivalence to a hypergeometric function. See
 * http://dlmf.nist.gov/8.17#ii
 *
 * @param[out] g1 d/da
 * @param[out] g2 d/db
 * @param[in] a a
 * @param[in] b b
 * @param[in] z z
 */
inline void grad_inc_beta(var& g1, var& g2, const var& a, const var& b,
                          const var& z) {
  var c1 = log(z);
  var c2 = log1m(z);
  var c3 = exp(lbeta(a, b)) * inc_beta(a, b, z);
  var C = exp(a * c1 + b * c2) / a;
  var dF1 = 0;
  var dF2 = 0;
  if (value_of(value_of(C)))
    grad_2F1(dF1, dF2, a + b, var(1.0), a + 1, z);
  g1 = (c1 - 1.0 / a) * c3 + C * (dF1 + dF2);
  g2 = c2 * c3 + C * dF1;
}

}  // namespace math
}  // namespace stan
#endif
