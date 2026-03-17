#ifndef STAN_MATH_PRIM_MAT_FUN_PROMOTE_SCALAR_TYPE_HPP
#define STAN_MATH_PRIM_MAT_FUN_PROMOTE_SCALAR_TYPE_HPP

#include <stan/math/prim/scal/fun/promote_scalar_type.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Template metaprogram to calculate a type for a matrix whose
 * underlying scalar is converted from the second template
 * parameter type to the first.
 *
 * This is the case for a vector container type.
 *
 * @tparam T result scalar type.
 * @tparam S input matrix scalar type
 */
template <typename T, typename S>
struct promote_scalar_type<T,
                           Eigen::Matrix<S, Eigen::Dynamic, Eigen::Dynamic> > {
  /**
   * The promoted type.
   */
  typedef Eigen::Matrix<typename promote_scalar_type<T, S>::type,
                        Eigen::Dynamic, Eigen::Dynamic>
      type;
};

/**
 * Template metaprogram to calculate a type for a vector whose
 * underlying scalar is converted from the second template
 * parameter type to the first.
 *
 * @tparam T result scalar type.
 * @tparam S input vector scalar type
 */
template <typename T, typename S>
struct promote_scalar_type<T, Eigen::Matrix<S, Eigen::Dynamic, 1> > {
  /**
   * The promoted type.
   */
  typedef Eigen::Matrix<typename promote_scalar_type<T, S>::type,
                        Eigen::Dynamic, 1>
      type;
};

/**
 * Template metaprogram to calculate a type for a row vector whose
 * underlying scalar is converted from the second template
 * parameter type to the first.
 *
 * @tparam T result scalar type.
 * @tparam S input row vector scalar type
 */
template <typename T, typename S>
struct promote_scalar_type<T, Eigen::Matrix<S, 1, Eigen::Dynamic> > {
  /**
   * The promoted type.
   */
  typedef Eigen::Matrix<typename promote_scalar_type<T, S>::type, 1,
                        Eigen::Dynamic>
      type;
};

}  // namespace math

}  // namespace stan

#endif
