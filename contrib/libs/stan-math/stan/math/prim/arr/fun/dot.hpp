#ifndef STAN_MATH_PRIM_ARR_FUN_DOT_HPP
#define STAN_MATH_PRIM_ARR_FUN_DOT_HPP

#include <vector>
#include <cstddef>

namespace stan {
namespace math {

inline double dot(const std::vector<double>& x, const std::vector<double>& y) {
  double sum = 0.0;
  for (size_t i = 0; i < x.size(); ++i)
    sum += x[i] * y[i];
  return sum;
}

}  // namespace math
}  // namespace stan
#endif
