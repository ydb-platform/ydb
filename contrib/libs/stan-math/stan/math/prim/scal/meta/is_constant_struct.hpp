#ifndef STAN_MATH_PRIM_SCAL_META_IS_CONSTANT_STRUCT_HPP
#define STAN_MATH_PRIM_SCAL_META_IS_CONSTANT_STRUCT_HPP

#include <stan/math/prim/scal/meta/is_constant.hpp>

namespace stan {

/**
 * Metaprogram to determine if a type has a base scalar
 * type that can be assigned to type double.
 * @tparam T Types to test
 */
template <typename T>
struct is_constant_struct {
  enum { value = is_constant<T>::value };
};

}  // namespace stan
#endif
