#ifndef STAN_MATH_PRIM_MAT_FUN_APPEND_ROW_HPP
#define STAN_MATH_PRIM_MAT_FUN_APPEND_ROW_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the result of stacking the rows of the first argument
 * matrix on top of the second argument matrix.
 *
 * The inputs can be
 * (matrix, matrix),
 * (matrix, row_vector),
 * (row_vector, matrix), or
 * (row_vector, row_vector),
 * and the output is always a matrix.
 *
 * @tparam T1 Scalar type of first matrix.
 * @tparam T2 Scalar type of second matrix.
 * @tparam R1 Row specification of first matrix.
 * @tparam C1 Column specification of first matrix.
 * @tparam R2 Row specification of second matrix.
 * @tparam C2 Column specification of second matrix.
 * @param A First matrix.
 * @param B Second matrix.
 * @return Result of stacking first matrix on top of second.
 */
template <typename T1, typename T2, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<typename return_type<T1, T2>::type, Eigen::Dynamic,
                     Eigen::Dynamic>
append_row(const Eigen::Matrix<T1, R1, C1>& A,
           const Eigen::Matrix<T2, R2, C2>& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  int Arows = A.rows();
  int Brows = B.rows();
  int Acols = A.cols();
  int Bcols = B.cols();
  check_size_match("append_row", "columns of A", Acols, "columns of B", Bcols);

  Matrix<typename return_type<T1, T2>::type, Dynamic, Dynamic> result(
      Arows + Brows, Acols);
  for (int j = 0; j < Acols; j++) {
    for (int i = 0; i < Arows; i++)
      result(i, j) = A(i, j);
    for (int i = Arows, k = 0; k < Brows; i++, k++)
      result(i, j) = B(k, j);
  }
  return result;
}

/**
 * Return the result of stacking the first vector on top of the
 * second vector, with the result being a vector.
 *
 * This function applies to (vector, vector) and returns a vector.
 *
 * @tparam T1 Scalar type of first vector.
 * @tparam T2 Scalar type of second vector.
 * @tparam R1 Row specification of first vector.
 * @tparam R2 Row specification of second vector.
 * @param A First vector.
 * @param B Second vector.
 * @return Result of stacking first vector on top of the second
 * vector.
 */
template <typename T1, typename T2, int R1, int R2>
inline Eigen::Matrix<typename return_type<T1, T2>::type, Eigen::Dynamic, 1>
append_row(const Eigen::Matrix<T1, R1, 1>& A,
           const Eigen::Matrix<T2, R2, 1>& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  int Asize = A.size();
  int Bsize = B.size();
  Matrix<typename return_type<T1, T2>::type, 1, Dynamic> result(Asize + Bsize);
  for (int i = 0; i < Asize; i++)
    result(i) = A(i);
  for (int i = 0, j = Asize; i < Bsize; i++, j++)
    result(j) = B(i);
  return result;
}

/**
 * Return the result of stacking the rows of the first argument
 * matrix on top of the second argument matrix.  This is an
 * overload for the case when the scalar types of the two input
 * matrix are the same.
 *
 * The inputs can be
 * (matrix, matrix),
 * (matrix, row_vector),
 * (row_vector, matrix), or
 * (row_vector, row_vector),
 * and the output is always a matrix.
 *
 * @tparam T Scalar type of both matrices.
 * @tparam R1 Row specification of first matrix.
 * @tparam C1 Column specification of first matrix.
 * @tparam R2 Row specification of second matrix.
 * @tparam C2 Column specification of second matrix.
 * @param A First matrix.
 * @param B Second matrix.
 * @return Result of stacking first matrix on top of second.
 */
template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> append_row(
    const Eigen::Matrix<T, R1, C1>& A, const Eigen::Matrix<T, R2, C2>& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  check_size_match("append_row", "columns of A", A.cols(), "columns of B",
                   B.cols());

  Matrix<T, Dynamic, Dynamic> result(A.rows() + B.rows(), A.cols());
  result << A, B;
  return result;
}

/**
 * Return the result of stacking the first vector on top of the
 * second vector, with the result being a vector.  This is an
 * overloaded template function for the case where both inputs
 * have the same scalar type.
 *
 * This function applies to (vector, vector) and returns a vector.
 *
 * @tparam T Scalar type of both vectors.
 * @tparam R1 Row specification of first vector.
 * @tparam R2 Row specification of second vector.
 * @param A First vector.
 * @param B Second vector.
 * @return Result of stacking first vector on top of the second
 * vector.
 */
template <typename T, int R1, int R2>
inline Eigen::Matrix<T, Eigen::Dynamic, 1> append_row(
    const Eigen::Matrix<T, R1, 1>& A, const Eigen::Matrix<T, R2, 1>& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  Matrix<T, Dynamic, 1> result(A.size() + B.size());
  result << A, B;
  return result;
}

/**
 * Return the result of stacking an scalar on top of the
 * a vector, with the result being a vector.
 *
 * This function applies to (scalar, vector) and returns a vector.
 *
 * @tparam T1 Scalar type of the scalar
 * @tparam T2 Scalar type of the vector.
 * @tparam R Row specification of the vector.
 * @param A scalar.
 * @param B vector.
 * @return Result of stacking the scalar on top of the vector.
 */
template <typename T1, typename T2, int R, int C>
inline Eigen::Matrix<typename return_type<T1, T2>::type, Eigen::Dynamic, 1>
append_row(const T1& A, const Eigen::Matrix<T2, R, C>& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  typedef typename return_type<T1, T2>::type return_type;

  Matrix<return_type, Dynamic, 1> result(B.size() + 1);
  result << A, B.template cast<return_type>();
  return result;
}

/**
 * Return the result of stacking a vector on top of the
 * an scalar, with the result being a vector.
 *
 * This function applies to (vector, scalar) and returns a vector.
 *
 * @tparam T1 Scalar type of the vector.
 * @tparam T2 Scalar type of the scalar
 * @tparam R Row specification of the vector.
 * @param A vector.
 * @param B scalar.
 * @return Result of stacking the vector on top of the scalar.
 */
template <typename T1, typename T2, int R, int C>
inline Eigen::Matrix<typename return_type<T1, T2>::type, Eigen::Dynamic, 1>
append_row(const Eigen::Matrix<T1, R, C>& A, const T2& B) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  typedef typename return_type<T1, T2>::type return_type;

  Matrix<return_type, Dynamic, 1> result(A.size() + 1);
  result << A.template cast<return_type>(), B;
  return result;
}

}  // namespace math

}  // namespace stan

#endif
