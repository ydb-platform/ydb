#ifndef STAN_MATH_PRIM_SCAL_META_GET_HPP
#define STAN_MATH_PRIM_SCAL_META_GET_HPP

#include <cmath>
#include <cstddef>

namespace stan {

template <typename T>
inline T get(const T& x, size_t n) {
  return x;
}

}  // namespace stan
#endif
