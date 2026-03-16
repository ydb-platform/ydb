#ifndef STAN_MATH_PRIM_MAT_FUN_GET_BASE1_LHS_HPP
#define STAN_MATH_PRIM_MAT_FUN_GET_BASE1_LHS_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_range.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one index.  If the index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i Index into vector plus 1.
 * @param error_msg Error message if the index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at <code>i - 1</code>
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(std::vector<T>& x, size_t i, const char* error_msg,
                        size_t idx) {
  check_range("[]", "x", x.size(), i, idx, error_msg);
  return x[i - 1];
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(std::vector<std::vector<T> >& x, size_t i1, size_t i2,
                        const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(std::vector<std::vector<std::vector<T> > >& x,
                        size_t i1, size_t i2, size_t i3, const char* error_msg,
                        size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param i4 Fourth index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(
    std::vector<std::vector<std::vector<std::vector<T> > > >& x, size_t i1,
    size_t i2, size_t i3, size_t i4, const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, i4, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param i4 Fourth index plus 1.
 * @param i5 Fifth index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(
    std::vector<std::vector<std::vector<std::vector<std::vector<T> > > > >& x,
    size_t i1, size_t i2, size_t i3, size_t i4, size_t i5,
    const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, i4, i5, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param i4 Fourth index plus 1.
 * @param i5 Fifth index plus 1.
 * @param i6 Sixth index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(
    std::vector<std::vector<
        std::vector<std::vector<std::vector<std::vector<T> > > > > >& x,
    size_t i1, size_t i2, size_t i3, size_t i4, size_t i5, size_t i6,
    const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, i4, i5, i6, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param i4 Fourth index plus 1.
 * @param i5 Fifth index plus 1.
 * @param i6 Sixth index plus 1.
 * @param i7 Seventh index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(
    std::vector<std::vector<std::vector<
        std::vector<std::vector<std::vector<std::vector<T> > > > > > >& x,
    size_t i1, size_t i2, size_t i3, size_t i4, size_t i5, size_t i6, size_t i7,
    const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, i4, i5, i6, i7, error_msg, idx + 1);
}

/**
 * Return a reference to the value of the specified vector at the
 * specified base-one indexes.  If an index is out of range, throw
 * a <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * @param x Vector from which to get a value.
 * @param i1 First index plus 1.
 * @param i2 Second index plus 1.
 * @param i3 Third index plus 1.
 * @param i4 Fourth index plus 1.
 * @param i5 Fifth index plus 1.
 * @param i6 Sixth index plus 1.
 * @param i7 Seventh index plus 1.
 * @param i8 Eigth index plus 1.
 * @param error_msg Error message if an index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of vector at indexes.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(
    std::vector<std::vector<std::vector<std::vector<
        std::vector<std::vector<std::vector<std::vector<T> > > > > > > >& x,
    size_t i1, size_t i2, size_t i3, size_t i4, size_t i5, size_t i6, size_t i7,
    size_t i8, const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), i1, idx, error_msg);
  return get_base1_lhs(x[i1 - 1], i2, i3, i4, i5, i6, i7, i8, error_msg,
                       idx + 1);
}

/**
 * Return a copy of the row of the specified vector at the specified
 * base-one row index.  If the index is out of range, throw a
 * <code>std::out_of_range</code> exception with the specified
 * error message and index indicated.
 *
 * <b>Warning</b>:  Because a copy is involved, it is inefficient
 * to access element of matrices by first using this method
 * to get a row then using a second call to get the value at
 a specified column.
 *
 * @param x Matrix from which to get a row
 * @param m Index into matrix plus 1.
 * @param error_msg Error message if the index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Row of matrix at <code>i - 1</code>.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline Eigen::Block<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> >
get_base1_lhs(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x, size_t m,
              const char* error_msg, size_t idx) {
  check_range("[]", "rows of x", x.rows(), m, idx, error_msg);
  return x.block(m - 1, 0, 1, x.cols());
}

/**
 * Return a reference to the value of the specified matrix at the specified
 * base-one row and column indexes.  If either index is out of range,
 * throw a <code>std::out_of_range</code> exception with the
 * specified error message and index indicated.
 *
 * @param x Matrix from which to get a row
 * @param m Row index plus 1.
 * @param n Column index plus 1.
 * @param error_msg Error message if either index is out of range.
 * @param idx Nested index level to report in error message if
 * either index is out of range.
 * @return Value of matrix at row <code>m - 1</code> and column
 * <code>n - 1</code>.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
                        size_t m, size_t n, const char* error_msg, size_t idx) {
  check_range("[]", "rows of x", x.rows(), m, idx, error_msg);
  check_range("[]", "cols of x", x.cols(), n, idx + 1, error_msg);
  return x(m - 1, n - 1);
}

/**
 * Return a reference to the value of the specified column vector
 * at the specified base-one index.  If the index is out of range,
 * throw a <code>std::out_of_range</code> exception with the
 * specified error message and index indicated.
 *
 * @param x Column vector from which to get a value.
 * @param m Row index plus 1.
 * @param error_msg Error message if the index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of column vector at row <code>m - 1</code>.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(Eigen::Matrix<T, Eigen::Dynamic, 1>& x, size_t m,
                        const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), m, idx, error_msg);
  return x(m - 1);
}

/**
 * Return a reference to the value of the specified row vector
 * at the specified base-one index.  If the index is out of range,
 * throw a <code>std::out_of_range</code> exception with the
 * specified error message and index indicated.
 *
 * @param x Row vector from which to get a value.
 * @param n Column index plus 1.
 * @param error_msg Error message if the index is out of range.
 * @param idx Nested index level to report in error message if
 * the index is out of range.
 * @return Value of row vector at column <code>n - 1</code>.
 * @tparam T type of value.
 * @throw std::out_of_range if idx is out of range.
 */
template <typename T>
inline T& get_base1_lhs(Eigen::Matrix<T, 1, Eigen::Dynamic>& x, size_t n,
                        const char* error_msg, size_t idx) {
  check_range("[]", "x", x.size(), n, idx, error_msg);
  return x(n - 1);
}

}  // namespace math
}  // namespace stan
#endif
