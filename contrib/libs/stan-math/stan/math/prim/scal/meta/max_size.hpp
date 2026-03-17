#ifndef STAN_MATH_PRIM_SCAL_META_MAX_SIZE_HPP
#define STAN_MATH_PRIM_SCAL_META_MAX_SIZE_HPP

#include <stan/math/prim/scal/meta/length.hpp>

namespace stan {

template <typename T1, typename T2>
size_t max_size(const T1& x1, const T2& x2) {
  size_t result = length(x1);
  result = result > length(x2) ? result : length(x2);
  return result;
}

template <typename T1, typename T2, typename T3>
size_t max_size(const T1& x1, const T2& x2, const T3& x3) {
  size_t result = length(x1);
  result = result > length(x2) ? result : length(x2);
  result = result > length(x3) ? result : length(x3);
  return result;
}

template <typename T1, typename T2, typename T3, typename T4>
size_t max_size(const T1& x1, const T2& x2, const T3& x3, const T4& x4) {
  size_t result = length(x1);
  result = result > length(x2) ? result : length(x2);
  result = result > length(x3) ? result : length(x3);
  result = result > length(x4) ? result : length(x4);
  return result;
}

template <typename T1, typename T2, typename T3, typename T4, typename T5>
size_t max_size(const T1& x1, const T2& x2, const T3& x3, const T4& x4,
                const T5& x5) {
  size_t result = length(x1);
  result = result > length(x2) ? result : length(x2);
  result = result > length(x3) ? result : length(x3);
  result = result > length(x4) ? result : length(x4);
  result = result > length(x5) ? result : length(x5);
  return result;
}

}  // namespace stan
#endif
