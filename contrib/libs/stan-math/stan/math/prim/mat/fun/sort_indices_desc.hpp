#ifndef STAN_MATH_PRIM_MAT_FUN_SORT_INDICES_DESC_HPP
#define STAN_MATH_PRIM_MAT_FUN_SORT_INDICES_DESC_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/mat/fun/sort_indices.hpp>
#include <algorithm>  // std::sort
#include <vector>

namespace stan {
namespace math {

/**
 * Return a sorted copy of the argument container in ascending order.
 *
 * @tparam C type of container
 * @param xs Container to sort
 * @return sorted version of container
 */
template <typename C>
std::vector<int> sort_indices_desc(const C& xs) {
  return sort_indices<false>(xs);
}

}  // namespace math
}  // namespace stan
#endif
