#ifndef STAN_MATH_PRIM_MAT_FUN_PROMOTE_ELEMENTS_HPP
#define STAN_MATH_PRIM_MAT_FUN_PROMOTE_ELEMENTS_HPP

#include <stan/math/prim/arr/fun/promote_elements.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This specialization promotes matrix elements of different types
 * which must be compatible with promotion.
 *
 * @tparam T type of promoted elements
 * @tparam S type of input elements, must be assignable to T
 */
template <typename T, typename S, int R, int C>
struct promote_elements<Eigen::Matrix<T, R, C>, Eigen::Matrix<S, R, C> > {
  /**
   * Return input matrix of type S as matrix of type T.
   *
   * @param u matrix of type S, assignable to type T
   * @returns matrix of type T
   */
  inline static Eigen::Matrix<T, R, C> promote(
      const Eigen::Matrix<S, R, C>& u) {
    Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> t(u.rows(), u.cols());
    for (int i = 0; i < u.size(); ++i)
      t(i) = promote_elements<T, S>::promote(u(i));
    return t;
  }
};

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This specialization promotes matrix elements of the same type.
 *
 * @tparam T type of elements
 */
template <typename T, int R, int C>
struct promote_elements<Eigen::Matrix<T, R, C>, Eigen::Matrix<T, R, C> > {
  /**
   * Return input matrix.
   *
   * @param u matrix of type T
   * @returns matrix of type T
   */
  inline static const Eigen::Matrix<T, R, C>& promote(
      const Eigen::Matrix<T, R, C>& u) {
    return u;
  }
};

}  // namespace math
}  // namespace stan

#endif
