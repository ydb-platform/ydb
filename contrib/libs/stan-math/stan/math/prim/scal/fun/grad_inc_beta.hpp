#ifndef STAN_MATH_PRIM_SCAL_FUN_GRAD_INC_BETA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_GRAD_INC_BETA_HPP

#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/fun/grad_2F1.hpp>
#include <cmath>

namespace stan {
namespace math {

// Gradient of the incomplete beta function beta(a, b, z)
// with respect to the first two arguments, using the
// equivalence to a hypergeometric function.
// See http://dlmf.nist.gov/8.17#ii
inline void grad_inc_beta(double& g1, double& g2, double a, double b,
                          double z) {
  using std::exp;
  using std::log;

  double c1 = log(z);
  double c2 = log1m(z);
  double c3 = exp(lbeta(a, b)) * inc_beta(a, b, z);
  double C = exp(a * c1 + b * c2) / a;
  double dF1 = 0;
  double dF2 = 0;
  if (C)
    grad_2F1(dF1, dF2, a + b, 1.0, a + 1, z);
  g1 = (c1 - 1.0 / a) * c3 + C * (dF1 + dF2);
  g2 = c2 * c3 + C * dF1;
}

}  // namespace math
}  // namespace stan
#endif
