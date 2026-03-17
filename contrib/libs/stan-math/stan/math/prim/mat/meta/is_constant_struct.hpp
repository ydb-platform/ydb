#ifndef STAN_MATH_PRIM_MAT_META_IS_CONSTANT_STRUCT_HPP
#define STAN_MATH_PRIM_MAT_META_IS_CONSTANT_STRUCT_HPP

#include <stan/math/prim/arr/meta/is_constant_struct.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/is_constant.hpp>

namespace stan {
/**
 * Defines a public enum named value and sets it to true(1)
 * if the type of the elements in the provided Eigen matrix
 * is a constant struct, false(0) otherwise.
 * @tparam type of the elements in the Eigen matrix
 * @tparam R number of rows in the provided matrix
 * @tparam C number of columns in the provided matrix
 */
template <typename T, int R, int C>
struct is_constant_struct<Eigen::Matrix<T, R, C> > {
  enum { value = is_constant_struct<T>::value };
};

/**
 * Defines a public enum named value and sets it to true(1)
 * if the type of the elements in the provided Eigen Block
 * is a constant struct, false(0) otherwise.
 * @tparam type of the elements in the Eigen Block
 */
template <typename T>
struct is_constant_struct<Eigen::Block<T> > {
  enum { value = is_constant_struct<T>::value };
};

}  // namespace stan
#endif
