#ifndef STAN_MODEL_INDEXING_LVALUE_HPP
#define STAN_MODEL_INDEXING_LVALUE_HPP

#include <boost/utility/enable_if.hpp>
#include <boost/type_traits/is_same.hpp>
#include <Eigen/Dense>
#include <stan/math/prim/mat.hpp>
#include <stan/model/indexing/index.hpp>
#include <stan/model/indexing/index_list.hpp>
#include <stan/model/indexing/rvalue_at.hpp>
#include <stan/model/indexing/rvalue_index_size.hpp>
#include <vector>

namespace stan {

  namespace model {

    /**
     * Assign the specified scalar reference under the specified
     * indexing to the specified scalar value.
     *
     * Types:  x[] <- y
     *
     * @tparam T Assigned variable type.
     * @tparam U Value type (must be assignable to T).
     * @param[in] x Variable to be assigned.
     * @param[in] y Value.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     */
    template <typename T, typename U>
    inline void assign(T& x, const nil_index_list& /* idxs */, const U& y,
                       const char* name = "ANON", int depth = 0) {
      x = y;
    }

    template <typename T, typename U, int R, int C>
    inline void assign(Eigen::Matrix<T, R, C>& x,
                       const nil_index_list& /* idxs */,
                       const Eigen::Matrix<U, R, C>& y,
                       const char* name = "ANON",
                       int depth = 0) {
      x.resize(y.rows(), y.cols());
      for (int i = 0; i < y.size(); ++i)
        assign(x(i), nil_index_list(), y(i), name, depth + 1);
    }


    template <typename T, typename U>
    inline void assign(std::vector<T>& x, const nil_index_list& /* idxs */,
                       const std::vector<U>& y, const char* name = "ANON",
                       int depth = 0) {
      x.resize(y.size());
      for (size_t i = 0; i < y.size(); ++i)
        assign(x[i], nil_index_list(), y[i], name, depth + 1);
    }


    /**
     * Assign the specified Eigen vector at the specified single index
     * to the specified value.
     *
     * Types: vec[uni] <- scalar
     *
     * @tparam T Type of assigned vector scalar.
     * @tparam U Type of value (must be assignable to T).
     * @param[in] x Vector variable to be assigned.
     * @param[in] idxs Sequence of one single index (from 1).
     * @param[in] y Value scalar.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If the index is out of bounds.
     */
    template <typename T, typename U>
    inline void assign(Eigen::Matrix<T, Eigen::Dynamic, 1>& x,
                       const cons_index_list<index_uni, nil_index_list>& idxs,
                       const U& y,
                       const char* name = "ANON", int depth = 0) {
      int i = idxs.head_.n_;
      math::check_range("vector[uni] assign range", name, x.size(), i);
      x(i - 1) = y;
    }

    /**
     * Assign the specified Eigen vector at the specified single index
     * to the specified value.
     *
     * Types:  row_vec[uni] <- scalar
     *
     * @tparam T Type of assigned row vector scalar.
     * @tparam U Type of value (must be assignable to T).
     * @param[in] x Row vector variable to be assigned.
     * @param[in] idxs Sequence of one single index (from 1).
     * @param[in] y Value scalar.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range Index is out of bounds.
     */
    template <typename T, typename U>
    inline void assign(Eigen::Matrix<T, 1, Eigen::Dynamic>& x,
                       const cons_index_list<index_uni, nil_index_list>& idxs,
                       const U& y,
                       const char* name = "ANON", int depth = 0) {
      int i = idxs.head_.n_;
      math::check_range("row_vector[uni] assign range", name, x.size(), i);
      x(i - 1) = y;
    }

    /**
     * Assign the specified Eigen vector at the specified multiple
     * index to the specified value.
     *
     * Types:  vec[multi] <- vec
     *
     * @tparam T Type of assigned vector scalar.
     * @tparam I Type of multiple index.
     * @tparam U Type of vector value scalar (must be assignable to T).
     * @param[in] x Row vector variable to be assigned.
     * @param[in] idxs Sequence of one single index (from 1).
     * @param[in] y Value vector.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the value size isn't the same as
     * the indexed size.
     */
    template <typename T, typename I, typename U>
    inline typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    assign(Eigen::Matrix<T, Eigen::Dynamic, 1>& x,
           const cons_index_list<I, nil_index_list>& idxs,
           const Eigen::Matrix<U, Eigen::Dynamic, 1>& y,
           const char* name = "ANON", int depth = 0) {
      math::check_size_match("vector[multi] assign sizes",
                             "lhs", rvalue_index_size(idxs.head_, x.size()),
                             name, y.size());
      for (int n = 0; n < y.size(); ++n) {
        int i = rvalue_at(n, idxs.head_);
        math::check_range("vector[multi] assign range", name, x.size(), i);
        x(i - 1) = y(n);
      }
    }

    /**
     * Assign the specified Eigen row vector at the specified multiple
     * index to the specified value.
     *
     * Types:   row_vec[multi] <- row_vec
     *
     * @tparam T Scalar type for assigned row vector.
     * @tparam I Type of multiple index.
     * @tparam U Type of value row vector scalar (must be assignable
     * to T).
     * @param[in] x Row vector variable to be assigned.
     * @param[in] idxs Sequence of one multiple index (from 1).
     * @param[in] y Value vector.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the value size isn't the same as
     * the indexed size.
     */
    template <typename T, typename I, typename U>
    inline typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    assign(Eigen::Matrix<T, 1, Eigen::Dynamic>& x,
           const cons_index_list<I, nil_index_list>& idxs,
           const Eigen::Matrix<U, 1, Eigen::Dynamic>& y,
           const char* name = "ANON", int depth = 0) {
      math::check_size_match("row_vector[multi] assign sizes",
                             "lhs", rvalue_index_size(idxs.head_, x.size()),
                             name, y.size());
      for (int n = 0; n < y.size(); ++n) {
        int i = rvalue_at(n, idxs.head_);
        math::check_range("row_vector[multi] assign range", name, x.size(), i);
        x(i - 1) = y(n);
      }
    }

    /**
     * Assign the specified Eigen matrix at the specified single index
     * to the specified row vector value.
     *
     * Types:  mat[uni] = rowvec
     *
     * @tparam T Assigned matrix scalar type.
     * @tparam U Type of value scalar for row vector (must be
     * assignable to T).
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Sequence of one single index (from 1).
     * @param[in] y Value row vector.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the number of columns in the row
     * vector and matrix do not match.
     */
    template <typename T, typename U>
    void assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
                const cons_index_list<index_uni, nil_index_list>& idxs,
                const Eigen::Matrix<U, 1, Eigen::Dynamic>& y,
                const char* name = "ANON", int depth = 0) {
      math::check_size_match("matrix[uni] assign sizes",
                             "lhs", x.cols(),
                             name, y.cols());
      int i = idxs.head_.n_;
      math::check_range("matrix[uni] assign range", name, x.rows(), i);
      for (int j = 0; j < x.cols(); ++j)  // loop allows double to var assgn
        x(i - 1, j) = y(j);
    }

    /**
     * Assign the specified Eigen matrix at the specified multiple
     * index to the specified matrix value.
     *
     * Types:  mat[multi] = mat
     *
     * @tparam T Assigned matrix scalar type.
     * @tparam I Multiple index type.
     * @tparam U Value matrix scalar type (must be assignable to T).
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Sequence of one multiple index (from 1).
     * @param[in] y Value matrix.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the dimensions of the indexed
     * matrix and right-hand side matrix do not match.
     */
    template <typename T, typename I, typename U>
    inline typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
           const cons_index_list<I, nil_index_list>& idxs,
           const Eigen::Matrix<U, Eigen::Dynamic, Eigen::Dynamic>& y,
           const char* name = "ANON", int depth = 0) {
      int x_idx_rows = rvalue_index_size(idxs.head_, x.rows());
      math::check_size_match("matrix[multi] assign row sizes",
                             "lhs", x_idx_rows,
                             name, y.rows());
      math::check_size_match("matrix[multi] assign col sizes",
                             "lhs", x.cols(),
                             name, y.cols());
      for (int i = 0; i < y.rows(); ++i) {
        int m = rvalue_at(i, idxs.head_);
        math::check_range("matrix[multi] assign range", name, x.rows(), m);
        // recurse to allow double to var assign
        for (int j = 0; j < x.cols(); ++j)
          x(m - 1, j) = y(i, j);
      }
    }

    /**
     * Assign the specified Eigen matrix at the specified pair of
     * single indexes to the specified scalar value.
     *
     * Types:  mat[single, single] = scalar
     *
     * @tparam T Matrix scalar type.
     * @tparam U Scalar type.
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Sequence of two single indexes (from 1).
     * @param[in] y Value scalar.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If either of the indices are out of bounds.
     */
    template <typename T, typename U>
    void assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
                const cons_index_list<index_uni,
                                      cons_index_list<index_uni,
                                                      nil_index_list> >& idxs,
                const U& y,
                const char* name = "ANON", int depth = 0) {
      int m = idxs.head_.n_;
      int n = idxs.tail_.head_.n_;
      math::check_range("matrix[uni,uni] assign range", name, x.rows(), m);
      math::check_range("matrix[uni,uni] assign range", name, x.cols(), n);
      x(m - 1, n - 1) = y;
    }

    /**
     * Assign the specified Eigen matrix at the specified single and
     * multiple index to the specified row vector.
     *
     * Types:  mat[uni, multi] = rowvec
     *
     * @tparam T Assigned matrix scalar type.
     * @tparam I Multi-index type.
     * @tparam U Value row vector scalar type (must be assignable to
     * T).
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Sequence of single and multiple index (from 1).
     * @param[in] y Value row vector.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the dimensions of the indexed
     * matrix and right-hand side row vector do not match.
     */
    template <typename T, typename I, typename U>
    inline typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
           const cons_index_list<index_uni,
                                 cons_index_list<I, nil_index_list> >& idxs,
           const Eigen::Matrix<U, 1, Eigen::Dynamic>& y,
           const char* name = "ANON", int depth = 0) {
      int x_idxs_cols = rvalue_index_size(idxs.tail_.head_, x.cols());
      math::check_size_match("matrix[uni,multi] assign sizes",
                             "lhs", x_idxs_cols,
                             name, y.cols());
      int m = idxs.head_.n_;
      math::check_range("matrix[uni,multi] assign range", name, x.rows(), m);
      for (int i = 0; i < y.size(); ++i) {
        int n = rvalue_at(i, idxs.tail_.head_);
        math::check_range("matrix[uni,multi] assign range", name, x.cols(), n);
        x(m - 1, n - 1) = y(i);
      }
    }

    /**
     * Assign the specified Eigen matrix at the specified multiple and
     * single index to the specified vector.
     *
     * Types:  mat[multi, uni] = vec
     *
     * @tparam T Assigned matrix scalar type.
     * @tparam I Multi-index type.
     * @tparam U Value vector scalar type (must be assignable to T).
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Sequence of multiple and single index (from 1).
     * @param[in] y Value vector.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the dimensions of the indexed
     * matrix and right-hand side vector do not match.
     */
    template <typename T, typename I, typename U>
    inline typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
           const cons_index_list<I,
                                 cons_index_list<index_uni,
                                                 nil_index_list> >& idxs,
           const Eigen::Matrix<U, Eigen::Dynamic, 1>& y,
           const char* name = "ANON", int depth = 0) {
      int x_idxs_rows = rvalue_index_size(idxs.head_, x.rows());
      math::check_size_match("matrix[multi,uni] assign sizes",
                             "lhs", x_idxs_rows,
                             name, y.rows());
      int n = idxs.tail_.head_.n_;
      math::check_range("matrix[multi,uni] assign range", name, x.cols(), n);
      for (int i = 0; i < y.size(); ++i) {
        int m = rvalue_at(i, idxs.head_);
        math::check_range("matrix[multi,uni] assign range", name, x.rows(), m);
        x(m - 1, n - 1) = y(i);
      }
    }

    /**
     * Assign the specified Eigen matrix at the specified pair of
     * multiple indexes to the specified matrix.
     *
     * Types:  mat[multi, multi] = mat
     *
     * @tparam T Assigned matrix scalar type.
     * @tparam I1 First multiple index type.
     * @tparam I2 Second multiple index type.
     * @tparam U Value matrix scalar type (must be assignable to T).
     * @param[in] x Matrix variable to be assigned.
     * @param[in] idxs Pair of multiple indexes (from 1).
     * @param[in] y Value matrix.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the dimensions of the indexed
     * matrix and value matrix do not match.
     */
    template <typename T, typename I1, typename I2, typename U>
    inline typename
    boost::disable_if_c<boost::is_same<I1, index_uni>::value
                        || boost::is_same<I2, index_uni>::value, void>::type
    assign(Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x,
           const cons_index_list<I1,
                                 cons_index_list<I2, nil_index_list> >& idxs,
           const Eigen::Matrix<U, Eigen::Dynamic, Eigen::Dynamic>& y,
           const char* name = "ANON", int depth = 0) {
      int x_idxs_rows = rvalue_index_size(idxs.head_, x.rows());
      int x_idxs_cols = rvalue_index_size(idxs.tail_.head_, x.cols());
      math::check_size_match("matrix[multi,multi] assign sizes",
                             "lhs", x_idxs_rows,
                             name, y.rows());
      math::check_size_match("matrix[multi,multi] assign sizes",
                             "lhs", x_idxs_cols,
                             name, y.cols());
      for (int j = 0; j < y.cols(); ++j) {
        int n = rvalue_at(j, idxs.tail_.head_);
        math::check_range("matrix[multi,multi] assign range", name,
                          x.cols(), n);
        for (int i = 0; i < y.rows(); ++i) {
          int m = rvalue_at(i, idxs.head_);
          math::check_range("matrix[multi,multi] assign range", name,
                            x.rows(), m);
          x(m - 1, n - 1) = y(i, j);
        }
      }
    }

    /**
     * Assign the specified array (standard vector) at the specified
     * index list beginning with a single index to the specified value.
     *
     * This function operates recursively to carry out the tail
     * indexing.
     *
     * Types:  x[uni | L] = y
     *
     * @tparam T Assigned vector member type.
     * @tparam L Type of tail of index list.
     * @tparam U Value scalar type (must be assignable to indexed
     * variable).
     * @param[in] x Array variable to be assigned.
     * @param[in] idxs List of indexes beginning with single index
     * (from 1).
     * @param[in] y Value.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the dimensions do not match in the
     * tail assignment.
     */
    template <typename T, typename L, typename U>
    inline void assign(std::vector<T>& x,
                       const cons_index_list<index_uni, L>& idxs, const U& y,
                       const char* name = "ANON", int depth = 0) {
      int i = idxs.head_.n_;
      math::check_range("vector[uni,...] assign range", name, x.size(), i);
      assign(x[i - 1], idxs.tail_, y, name, depth + 1);
    }

    /**
     * Assign the specified array (standard vector) at the specified
     * index list beginning with a multiple index to the specified value.
     *
     * This function operates recursively to carry out the tail
     * indexing.
     *
     * Types:  x[multi | L] = y
     *
     * @tparam T Assigned vector member type.
     * @tparam I Type of multiple index heading index list.
     * @tparam L Type of tail of index list.
     * @tparam U Value scalar type (must be assignable to indexed
     * variable).
     * @param[in] x Array variable to be assigned.
     * @param[in] idxs List of indexes beginning with multiple index
     * (from 1).
     * @param[in] y Value.
     * @param[in] name Name of variable (default "ANON").
     * @param[in] depth Indexing depth (default 0).
     * @throw std::out_of_range If any of the indices are out of bounds.
     * @throw std::invalid_argument If the size of the multiple indexing
     * and size of first dimension of value do not match, or any of
     * the recursive tail assignment dimensions do not match.
     */
    template <typename T, typename I, typename L, typename U>
    typename boost::disable_if<boost::is_same<I, index_uni>, void>::type
    inline assign(std::vector<T>& x, const cons_index_list<I, L>& idxs,
                  const std::vector<U>& y,
                  const char* name = "ANON", int depth = 0) {
      int x_idx_size = rvalue_index_size(idxs.head_, x.size());
      math::check_size_match("vector[multi,...] assign sizes",
                             "lhs", x_idx_size,
                             name, y.size());
      for (size_t n = 0; n < y.size(); ++n) {
        int i = rvalue_at(n, idxs.head_);
        math::check_range("vector[multi,...] assign range", name, x.size(), i);
        assign(x[i - 1], idxs.tail_, y[n], name, depth + 1);
      }
    }

  }
}
#endif
