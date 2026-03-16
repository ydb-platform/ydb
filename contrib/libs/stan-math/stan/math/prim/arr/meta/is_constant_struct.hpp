#ifndef STAN_MATH_PRIM_ARR_META_IS_CONSTANT_STRUCT_HPP
#define STAN_MATH_PRIM_ARR_META_IS_CONSTANT_STRUCT_HPP

#include <stan/math/prim/scal/meta/is_constant.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <vector>

namespace stan {
/**
 * Defines a public enum named value and sets it to true(1)
 * if the type of the elements in the provided std::vector
 * is a constant struct, false(0) otherwise.
 * @tparam type of the elements in the std::vector
 */
template <typename T>
struct is_constant_struct<std::vector<T> > {
  enum { value = is_constant_struct<T>::value };
};

}  // namespace stan
#endif
