#ifndef STAN_MATH_PRIM_MAT_META_SCALAR_TYPE_HPP
#define STAN_MATH_PRIM_MAT_META_SCALAR_TYPE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/arr/meta/scalar_type.hpp>

namespace stan {
/**
 * Template metaprogram defining the base scalar type of
 * values stored in an Eigen matrix.
 *
 * @tparam T type of matrix.
 * @tparam R number of rows for matrix.
 * @tparam C number of columns for matrix.
 */
template <typename T, int R, int C>
struct scalar_type<Eigen::Matrix<T, R, C> > {
  typedef typename scalar_type<T>::type type;
};

/**
 * Template metaprogram defining the base scalar type of
 * values stored in a const Eigen matrix.
 *
 * @tparam T type of matrix.
 * @tparam R number of rows for matrix.
 * @tparam C number of columns for matrix.
 */
template <typename T, int R, int C>
struct scalar_type<const Eigen::Matrix<T, R, C> > {
  typedef typename scalar_type<T>::type type;
};

/**
 * Template metaprogram defining the base scalar type of
 * values stored in a referenced  Eigen matrix.
 *
 * @tparam T type of matrix.
 * @tparam R number of rows for matrix.
 * @tparam C number of columns for matrix.
 */
template <typename T, int R, int C>
struct scalar_type<Eigen::Matrix<T, R, C>&> {
  typedef typename scalar_type<T>::type type;
};

/**
 * Template metaprogram defining the base scalar type of
 * values stored in a referenced const Eigen matrix.
 *
 * @tparam T type of matrix.
 * @tparam R number of rows for matrix.
 * @tparam C number of columns for matrix.
 */
template <typename T, int R, int C>
struct scalar_type<const Eigen::Matrix<T, R, C>&> {
  typedef typename scalar_type<T>::type type;
};

/**
 * Template metaprogram defining the base scalar type of
 * values stored in an Eigen Block.
 *
 * @tparam T type of block.
 */
template <typename T>
struct scalar_type<Eigen::Block<T> > {
  typedef typename scalar_type<T>::type type;
};
}  // namespace stan
#endif
