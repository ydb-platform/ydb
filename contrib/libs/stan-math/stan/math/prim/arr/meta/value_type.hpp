#ifndef STAN_MATH_PRIM_ARR_META_VALUE_TYPE_HPP
#define STAN_MATH_PRIM_ARR_META_VALUE_TYPE_HPP

#include <stan/math/prim/scal/meta/value_type.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Template metaprogram class to compute the type of values stored
 * in a standard vector.
 *
 * @tparam T type of elements in standard vector.
 */
template <typename T>
struct value_type<std::vector<T> > {
  /**
   * Type of value stored in a standard vector with type
   * <code>T</code> entries.
   */
  typedef T type;
};

}  // namespace math
}  // namespace stan
#endif
