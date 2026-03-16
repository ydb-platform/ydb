#ifndef STAN_MATH_PRIM_MAT_FUN_PROMOTE_SCALAR_HPP
#define STAN_MATH_PRIM_MAT_FUN_PROMOTE_SCALAR_HPP

#include <stan/math/prim/scal/fun/promote_scalar.hpp>
#include <stan/math/prim/mat/fun/promote_scalar_type.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Struct to hold static function for promoting underlying scalar
 * types.  This specialization is for Eigen matrix inputs.
 *
 * @tparam T return scalar type
 * @tparam S input matrix scalar type for static nested function, which must
 * have a scalar type assignable to T
 */
template <typename T, typename S>
struct promote_scalar_struct<T, Eigen::Matrix<S, -1, -1> > {
  /**
   * Return the matrix consisting of the recursive promotion of
   * the elements of the input matrix to the scalar type specified
   * by the return template parameter.
   *
   * @param x input matrix.
   * @return matrix with values promoted from input vector.
   */
  static Eigen::Matrix<typename promote_scalar_type<T, S>::type, -1, -1> apply(
      const Eigen::Matrix<S, -1, -1>& x) {
    Eigen::Matrix<typename promote_scalar_type<T, S>::type, -1, -1> y(x.rows(),
                                                                      x.cols());
    for (int i = 0; i < x.size(); ++i)
      y(i) = promote_scalar_struct<T, S>::apply(x(i));
    return y;
  }
};

/**
 * Struct to hold static function for promoting underlying scalar
 * types.  This specialization is for Eigen column vector inputs.
 *
 * @tparam T return scalar type
 * @tparam S input matrix scalar type for static nested function, which must
 * have a scalar type assignable to T
 */
template <typename T, typename S>
struct promote_scalar_struct<T, Eigen::Matrix<S, 1, -1> > {
  /**
   * Return the column vector consisting of the recursive promotion of
   * the elements of the input column vector to the scalar type specified
   * by the return template parameter.
   *
   * @param x input column vector.
   * @return column vector with values promoted from input vector.
   */
  static Eigen::Matrix<typename promote_scalar_type<T, S>::type, 1, -1> apply(
      const Eigen::Matrix<S, 1, -1>& x) {
    Eigen::Matrix<typename promote_scalar_type<T, S>::type, 1, -1> y(x.rows(),
                                                                     x.cols());
    for (int i = 0; i < x.size(); ++i)
      y(i) = promote_scalar_struct<T, S>::apply(x(i));
    return y;
  }
};

/**
 * Struct to hold static function for promoting underlying scalar
 * types.  This specialization is for Eigen row vector inputs.
 *
 * @tparam T return scalar type
 * @tparam S input matrix scalar type for static nested function, which must
 * have a scalar type assignable to T
 */
template <typename T, typename S>
struct promote_scalar_struct<T, Eigen::Matrix<S, -1, 1> > {
  /**
   * Return the row vector consisting of the recursive promotion of
   * the elements of the input row vector to the scalar type specified
   * by the return template parameter.
   *
   * @param x input row vector.
   * @return row vector with values promoted from input vector.
   */
  static Eigen::Matrix<typename promote_scalar_type<T, S>::type, -1, 1> apply(
      const Eigen::Matrix<S, -1, 1>& x) {
    Eigen::Matrix<typename promote_scalar_type<T, S>::type, -1, 1> y(x.rows(),
                                                                     x.cols());
    for (int i = 0; i < x.size(); ++i)
      y(i) = promote_scalar_struct<T, S>::apply(x(i));
    return y;
  }
};

}  // namespace math
}  // namespace stan

#endif
