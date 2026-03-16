#ifndef STAN_MATH_PRIM_MAT_FUN_INITIALIZE_HPP
#define STAN_MATH_PRIM_MAT_FUN_INITIALIZE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>
#include <type_traits>

namespace stan {
namespace math {

// initializations called for local variables generate in Stan
// code; fills in all cells in first arg with second arg

template <typename T>
inline void initialize(T& x, const T& v) {
  x = v;
}
template <typename T, typename V>
inline typename std::enable_if<std::is_arithmetic<V>::value, void>::type
initialize(T& x, V v) {
  x = v;
}
template <typename T, int R, int C, typename V>
inline void initialize(Eigen::Matrix<T, R, C>& x, const V& v) {
  for (int i = 0; i < x.size(); ++i)
    initialize(x(i), v);
}
template <typename T, typename V>
inline void initialize(std::vector<T>& x, const V& v) {
  for (size_t i = 0; i < x.size(); ++i)
    initialize(x[i], v);
}

}  // namespace math
}  // namespace stan
#endif
