#ifndef STAN_MATH_FWD_MAT_FUN_SQUARED_DISTANCE_HPP
#define STAN_MATH_FWD_MAT_FUN_SQUARED_DISTANCE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/fwd/core/operator_subtraction.hpp>
#include <stan/math/fwd/mat/fun/dot_self.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <stan/math/prim/mat/fun/subtract.hpp>

namespace stan {
namespace math {

/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R Rows at compile time of vector inputs
 * @tparam C columns at compile time of vector inputs
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R, int C>
inline fvar<T> squared_distance(const Eigen::Matrix<fvar<T>, R, C>& v1,
                                const Eigen::Matrix<double, R, C>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<fvar<T>, R, C> v3 = subtract(v1, v2);
  return dot_self(v3);
}

/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R1 Rows at compile time of first vector input
 * @tparam C1 Columns at compile time of first vector input
 * @tparam R2 Rows at compile time of second vector input
 * @tparam C2 Columns at compile time of second vector input
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> squared_distance(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                                const Eigen::Matrix<double, R2, C2>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<double, R1, C1> t_v2 = v2.transpose();
  Eigen::Matrix<fvar<T>, R1, C1> v3 = subtract(v1, t_v2);
  return dot_self(v3);
}

/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R Rows at compile time of vector inputs
 * @tparam C columns at compile time of vector inputs
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R, int C>
inline fvar<T> squared_distance(const Eigen::Matrix<double, R, C>& v1,
                                const Eigen::Matrix<fvar<T>, R, C>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<fvar<T>, R, C> v3 = subtract(v1, v2);
  return dot_self(v3);
}

/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R1 Rows at compile time of first vector input
 * @tparam C1 Columns at compile time of first vector input
 * @tparam R2 Rows at compile time of second vector input
 * @tparam C2 Columns at compile time of second vector input
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> squared_distance(const Eigen::Matrix<double, R1, C1>& v1,
                                const Eigen::Matrix<fvar<T>, R2, C2>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<double, R2, C2> t_v1 = v1.transpose();
  Eigen::Matrix<fvar<T>, R2, C2> v3 = subtract(t_v1, v2);
  return dot_self(v3);
}
/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R Rows at compile time of vector inputs
 * @tparam C columns at compile time of vector inputs
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R, int C>
inline fvar<T> squared_distance(const Eigen::Matrix<fvar<T>, R, C>& v1,
                                const Eigen::Matrix<fvar<T>, R, C>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<fvar<T>, R, C> v3 = subtract(v1, v2);
  return dot_self(v3);
}

/**
 * Returns the squared distance between the specified vectors
 * of the same dimensions.
 *
 * @tparam R1 Rows at compile time of first vector input
 * @tparam C1 Columns at compile time of first vector input
 * @tparam R2 Rows at compile time of second vector input
 * @tparam C2 Columns at compile time of second vector input
 * @tparam T Child scalar type of fvar vector input
 * @param v1 First vector.
 * @param v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error If the vectors are not the same
 * size or if they are both not vector dimensioned.
 */
template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> squared_distance(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                                const Eigen::Matrix<fvar<T>, R2, C2>& v2) {
  check_vector("squared_distance", "v1", v1);
  check_vector("squared_distance", "v2", v2);
  check_matching_sizes("squared_distance", "v1", v1, "v2", v2);
  Eigen::Matrix<fvar<T>, R2, C2> t_v1 = v1.transpose();
  Eigen::Matrix<fvar<T>, R2, C2> v3 = subtract(t_v1, v2);
  return dot_self(v3);
}

}  // namespace math
}  // namespace stan
#endif
