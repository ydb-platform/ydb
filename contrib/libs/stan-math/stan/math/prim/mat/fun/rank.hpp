#ifndef STAN_MATH_PRIM_MAT_FUN_RANK_HPP
#define STAN_MATH_PRIM_MAT_FUN_RANK_HPP

#include <stan/math/prim/mat/err/check_range.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>

namespace stan {
namespace math {

/**
 * Return the number of components of v less than v[s].
 *
 * @tparam C container type
 * @param[in] v input vector
 * @param[in] s position in vector
 * @return number of components of v less than v[s].
 * @throw std::out_of_range if s is out of range.
 */
template <typename C>
inline int rank(const C& v, int s) {
  check_range("rank", "v", v.size(), s);
  --s;  // adjust for indexing by one
  int count = 0;
  for (typename index_type<C>::type i = 0; i < v.size(); ++i)
    if (v[i] < v[s])
      ++count;
  return count;
}

}  // namespace math
}  // namespace stan
#endif
