#ifndef STAN_MATH_PRIM_ARR_META_INDEX_TYPE_HPP
#define STAN_MATH_PRIM_ARR_META_INDEX_TYPE_HPP

#include <stan/math/prim/scal/meta/index_type.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Template metaprogram class to compute the type of index for a
 * standard vector.
 *
 * @tparam T type of elements in standard vector.
 */
template <typename T>
struct index_type<std::vector<T> > {
  /**
   * Typedef for index of standard vectors.
   */
  typedef typename std::vector<T>::size_type type;
};

}  // namespace math
}  // namespace stan

#endif
