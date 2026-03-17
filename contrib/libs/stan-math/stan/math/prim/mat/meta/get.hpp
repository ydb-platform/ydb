#ifndef STAN_MATH_PRIM_MAT_META_GET_HPP
#define STAN_MATH_PRIM_MAT_META_GET_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {

template <typename T, int R, int C>
inline T get(const Eigen::Matrix<T, R, C>& m, size_t n) {
  return m(static_cast<int>(n));
}

template <typename T, int R, int C>
inline T get(const Eigen::Array<T, R, C>& m, size_t n) {
  return m(static_cast<int>(n));
}

}  // namespace stan
#endif
