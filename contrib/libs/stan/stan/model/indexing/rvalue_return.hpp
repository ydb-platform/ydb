#ifndef STAN_MODEL_INDEXING_RVALUE_RETURN_HPP
#define STAN_MODEL_INDEXING_RVALUE_RETURN_HPP

#include <Eigen/Dense>
#include <stan/model/indexing/index.hpp>
#include <stan/model/indexing/index_list.hpp>
#include <vector>

namespace stan {

  namespace model {

    /**
     * Primary template class for metaprogram to calculate return
     * value for <code>model::rvalue()</code> for the container or
     * scalar type and index list type specified in the template
     * parameters. 
     *
     * <p>Specializations of this class will define a typedef
     * <code>type</code> for the return type.
     *
     * @tparam C Type of container or scalar.
     * @tparam L Type of index list.
     */
    template <typename C, typename L>
    struct rvalue_return {
    };

    /**
     * Template class specialization for nil indexes, which provide
     * the container type as the return type.  The container type may
     * be a scalar type or a container.
     *
     * @tparam C Container or scalar type.
     */
    template <typename C>
    struct rvalue_return<C, nil_index_list> {
      /**
       * Return type is the container or scalar type.
       */
      typedef C type;
    };

    // SINGLE INDEX

    /**
     * Template class specialization for an Eigen matrix, vector or
     * rwo vector and one multiple index. 
     *
     * @tparam T Type of scalar in matrix.
     * @tparam I Type of first index (only instantiated to multi
     * indexes).
     * @tparam R Rows for matrix.
     * @tparam C Columns for matrix.
     */
    template <typename T, typename I, int R, int C>
    struct rvalue_return<Eigen::Matrix<T, R, C>,
                         cons_index_list<I, nil_index_list> > {
      /**
       * Return type is the matrix container type.
       */
      typedef Eigen::Matrix<T, R, C> type;
    };

    /**
     * Template class specialization for an Eigen matrix and one single
     * index. 
     *
     * @tparam T Type of scalar in matrix.
     */
    template <typename T>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>,
                         cons_index_list<index_uni, nil_index_list> > {
      /**
       * Return type is row vector.
       */
      typedef Eigen::Matrix<T, 1, Eigen::Dynamic> type;
    };

    /**
     * Template class specialization for an Eigen vector and one single
     * index. 
     *
     * @tparam T Type of scalar in vector.
     */
    template <typename T>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, 1>,
                         cons_index_list<index_uni, nil_index_list> > {
      /**
       * Return type is scalar type of vector.
       */
      typedef T type;
    };

    /**
     * Template class specialization for an Eigen row vector and one
     * single index. 
     *
     * @tparam T Type of scalar in row vector. 
     */
    template <typename T>
    struct rvalue_return<Eigen::Matrix<T, 1, Eigen::Dynamic>,
                         cons_index_list<index_uni, nil_index_list> > {
      /**
       * Return type is scalar type of row vector. 
       */
      typedef T type;
    };

    /**
     * Template specialization for an Eigen matrix and two multiple
     * indexes.  
     *
     * @tparam T Type of scalar in matrix.
     * @tparam I1 Type of first multiple index.
     * @tparam I2 Type of second multiple index.
     */
    template <typename T, typename I1, typename I2>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>,
                         cons_index_list<I1,
                                         cons_index_list<I2,
                                                         nil_index_list> > > {
      /**
       * Return type is matrix container type.
       */
      typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> type;
    };

    /**
     * Template specialization for an Eigen matrix with one multiple
     * index followed by one single index.
     *
     * @tparam T Type of scalar in matrix.
     * @tparam I Type of multiple index.
     */
    template <typename T, typename I>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>,
                         cons_index_list<I,
                                         cons_index_list<index_uni,
                                                         nil_index_list> > > {
      /**
       * Return type is vector with same scalar type as matrix container.
       */
      typedef Eigen::Matrix<T, Eigen::Dynamic, 1> type;
    };

    /**
     * Template specialization for an Eigen matrix with one single
     * index followed by one multiple index.
     *
     * @tparam T Type of scalar in matrix.
     * @tparam I Type of multiple index.
     */
    template <typename T, typename I>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>,
                         cons_index_list<index_uni,
                                         cons_index_list<I,
                                                         nil_index_list> > > {
      /**
       * Return type is row vector with same scalar type as matrix container.
       */
      typedef Eigen::Matrix<T, 1, Eigen::Dynamic> type;
    };

    /**
     * Template specialization for an Eigen matrix with two single
     * indexes.
     *
     * @tparam T Type of scalar in matrix.
     */
    template <typename T>
    struct rvalue_return<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>,
                         cons_index_list<index_uni,
                                         cons_index_list<index_uni,
                                                         nil_index_list> > > {
      /**
       * Return type is scalar type of matrix.
       */
      typedef T type;
    };

    /**
     * Template specialization for a standard vector whose index list
     * starts with a multiple index.
     *
     * @tparam C Element type for standard vector (container or
     * scalar).
     * @tparam I Multiple index type.
     * @tparam L Following index types.
     */
    template <typename C, typename I, typename L>
    struct rvalue_return<std::vector<C>, cons_index_list<I, L> > {
      /**
       * Return type is calculated recursively as a standard vector of
       * the rvalue return for the element type C and following index
       * types L.
       */
      typedef std::vector<typename rvalue_return<C, L>::type> type;
    };


    /**
     * Template specialization for a standard vector whose index list
     * starts with a single index.
     *
     * @tparam C Element type for standard vector (container or
     * scalar).
     * @tparam L Following index types.
     */
    template <typename C, typename L>
    struct rvalue_return<std::vector<C>, cons_index_list<index_uni, L> > {
      /**
       * Return type is calculated recursively as the rvalue return
       * for the element type C and following index types L.
       */
      typedef typename rvalue_return<C, L>::type type;
    };

  }
}
#endif
