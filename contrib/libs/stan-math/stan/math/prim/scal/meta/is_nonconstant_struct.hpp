#ifndef STAN_MATH_PRIM_SCAL_META_IS_NONCONSTANT_STRUCT_HPP
#define STAN_MATH_PRIM_SCAL_META_IS_NONCONSTANT_STRUCT_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>

namespace stan {

/**
 * Defines a public enum named value which is defined to be false (0)
 * if the type has a base scalar type that can be assigned to type double
 * and true (1) otherwise.
 */
template <typename T>
struct is_nonconstant_struct {
  enum { value = !stan::is_constant_struct<T>::value };
};

}  // namespace stan
#endif
