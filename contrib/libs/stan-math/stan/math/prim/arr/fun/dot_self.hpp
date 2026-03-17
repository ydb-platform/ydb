#ifndef STAN_MATH_PRIM_ARR_FUN_DOT_SELF_HPP
#define STAN_MATH_PRIM_ARR_FUN_DOT_SELF_HPP

#include <vector>
#include <cstddef>

namespace stan {
namespace math {

inline double dot_self(const std::vector<double>& x) {
  double sum = 0.0;
  for (double i : x)
    sum += i * i;
  return sum;
}

}  // namespace math
}  // namespace stan
#endif
