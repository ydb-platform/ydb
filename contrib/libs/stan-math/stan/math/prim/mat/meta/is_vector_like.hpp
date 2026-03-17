#ifndef STAN_MATH_PRIM_MAT_META_IS_VECTOR_LIKE_HPP
#define STAN_MATH_PRIM_MAT_META_IS_VECTOR_LIKE_HPP

#include <stan/math/prim/scal/meta/is_vector_like.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {

/**
 * Template metaprogram indicates whether a type is vector_like.
 *
 * A type is vector_like if an instance can be accessed like a
 * vector, i.e. square brackets.
 *
 * Access is_vector_like::value for the result.
 *
 * This metaprogram removes the const qualifier.
 *
 * @tparam T Type to test
 */
template <typename T, int R, int C>
struct is_vector_like<Eigen::Matrix<T, R, C> > {
  enum { value = true };
};

/**
 * Template metaprogram indicates whether a type is vector_like.
 *
 * A type is vector_like if an instance can be accessed like a
 * vector, i.e. square brackets.
 *
 * Access is_vector_like::value for the result.
 *
 * This metaprogram removes the const qualifier.
 *
 * @tparam T Type to test
 */
template <typename T, int R, int C>
struct is_vector_like<Eigen::Array<T, R, C> > {
  enum { value = true };
};
}  // namespace stan
#endif
