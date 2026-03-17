#ifndef STAN_MATH_PRIM_SCAL_META_VALUE_TYPE_HPP
#define STAN_MATH_PRIM_SCAL_META_VALUE_TYPE_HPP

namespace stan {
namespace math {

/**
 * Primary template class for metaprogram to compute the type of
 * values stored in a container.
 *
 * Only the specializations have behavior that can be used, and
 * all implement a typedef <code>type</code> for the type of the
 * values in the container.
 *
 * tparam T type of container.
 */
template <typename T>
struct value_type {};

/**
 * Template class for metaprogram to compute the type of values
 * stored in a constant container.
 *
 * @tparam T type of container without const modifier.
 */
template <typename T>
struct value_type<const T> {
  typedef typename value_type<T>::type type;
};

}  // namespace math
}  // namespace stan
#endif
