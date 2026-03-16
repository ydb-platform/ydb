#ifndef STAN_MATH_FWD_ARR_FUN_SUM_HPP
#define STAN_MATH_FWD_ARR_FUN_SUM_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/arr/fun/sum.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the sum of the entries of the specified standard
 * vector.
 *
 * @tparam T Type of vector entries.
 * @param m Vector.
 * @return Sum of vector entries.
 */
template <typename T>
inline fvar<T> sum(const std::vector<fvar<T> >& m) {
  if (m.size() == 0)
    return 0.0;
  std::vector<T> vals(m.size());
  std::vector<T> tans(m.size());
  for (size_t i = 0; i < m.size(); ++i) {
    vals[i] = m[i].val();
    tans[i] = m[i].tangent();
  }
  return fvar<T>(sum(vals), sum(tans));
}

}  // namespace math
}  // namespace stan
#endif
