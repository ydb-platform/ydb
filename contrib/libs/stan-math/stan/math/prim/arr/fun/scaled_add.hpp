#ifndef STAN_MATH_PRIM_ARR_FUN_SCALED_ADD_HPP
#define STAN_MATH_PRIM_ARR_FUN_SCALED_ADD_HPP

#include <vector>
#include <cstddef>

namespace stan {
namespace math {

inline void scaled_add(std::vector<double>& x, const std::vector<double>& y,
                       double lambda) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] += lambda * y[i];
}

}  // namespace math
}  // namespace stan

#endif
