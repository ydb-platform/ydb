#ifndef STAN_MATH_PRIM_ARR_FUN_REP_ARRAY_HPP
#define STAN_MATH_PRIM_ARR_FUN_REP_ARRAY_HPP

#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T>
inline std::vector<T> rep_array(const T& x, int n) {
  check_nonnegative("rep_array", "n", n);
  return std::vector<T>(n, x);
}

template <typename T>
inline std::vector<std::vector<T> > rep_array(const T& x, int m, int n) {
  using std::vector;
  check_nonnegative("rep_array", "rows", m);
  check_nonnegative("rep_array", "cols", n);
  return vector<vector<T> >(m, vector<T>(n, x));
}

template <typename T>
inline std::vector<std::vector<std::vector<T> > > rep_array(const T& x, int k,
                                                            int m, int n) {
  using std::vector;
  check_nonnegative("rep_array", "shelfs", k);
  check_nonnegative("rep_array", "rows", m);
  check_nonnegative("rep_array", "cols", n);
  return vector<vector<vector<T> > >(k, vector<vector<T> >(m, vector<T>(n, x)));
}

}  // namespace math
}  // namespace stan

#endif
