#ifndef STAN_MATH_PRIM_SCAL_FUN_AS_COLUMN_VECTOR_OR_SCALAR_HPP
#define STAN_MATH_PRIM_SCAL_FUN_AS_COLUMN_VECTOR_OR_SCALAR_HPP

namespace stan {
namespace math {

/**
 * Converts input argument to a column vector or a scalar. For scalar inputs
 * that is an identity function.
 *
 * @tparam T Type of scalar element.
 * @param a Specified scalar.
 * @return 1x1 matrix that contains the value of scalar.
 */
template <typename T>
inline const T& as_column_vector_or_scalar(const T& a) {
  return a;
}

}  // namespace math
}  // namespace stan

#endif
