#ifndef STAN_MATH_PRIM_SCAL_META_SCALAR_TYPE_HPP
#define STAN_MATH_PRIM_SCAL_META_SCALAR_TYPE_HPP

#include <stan/math/prim/scal/meta/is_vector.hpp>
#include <stan/math/prim/scal/meta/value_type.hpp>

namespace stan {
/**
 * Metaprogram structure to determine the base scalar type
 * of a template argument.
 *
 * <p>This base class should be specialized for structured types.
 *
 * @tparam T Type of object.
 */
template <typename T>
struct scalar_type {
  typedef T type;
};

template <typename T>
struct scalar_type<T*> {
  typedef typename scalar_type<T>::type type;
};

}  // namespace stan
#endif
