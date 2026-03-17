#ifndef STAN_MATH_REV_SCAL_FUN_CALCULATE_CHAIN_HPP
#define STAN_MATH_REV_SCAL_FUN_CALCULATE_CHAIN_HPP

#include <valarray>

namespace stan {
namespace math {

inline double calculate_chain(double x, double val) {
  return std::exp(x - val);  // works out to inv_logit(x)
}

}  // namespace math
}  // namespace stan
#endif
