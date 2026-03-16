#ifndef STAN_MODEL_INDEXING_DEEP_COPY_HPP
#define STAN_MODEL_INDEXING_DEEP_COPY_HPP

#include <Eigen/Dense>
#include <vector>

namespace stan {

  namespace model {

    /**
     * Return the specified argument as a constant reference.
     *
     * <p>Warning: because of the usage pattern of this class, this
     * function only needs to return value references, not actual
     * copies.  The functions that call this overload recursively will
     * be doing the actual copies with assignment.
     *
     * @tparam T Type of scalar.
     * @param x Input value.
     * @return Constant reference to input.
     */
    template <typename T>
    inline const T& deep_copy(const T& x) {
      return x;
    }

    /**
     * Return a copy of the specified matrix, vector, or row
     * vector.  The return value is a copy in the sense that modifying
     * its contents will not affect the original matrix.
     *
     * <p>Warning:  This function assumes that the elements of the
     * matrix deep copy under assignment.
     *
     * @tparam T Scalar type.
     * @tparam R Row type specificiation.
     * @tparam C Column type specificiation.
     * @param a Input matrix, vector, or row vector.
     * @return Deep copy of input.
     */
    template <typename T, int R, int C>
    inline Eigen::Matrix<T, R, C> deep_copy(const Eigen::Matrix<T, R, C>& a) {
      Eigen::Matrix<T, R, C> result(a);
      return result;
    }

    /**
     * Return a deep copy of the specified standard vector.  The
     * return value is a copy in the sense that modifying its contents
     * will not affect the original vector.
     *
     * <p>Warning:  This function assumes that the elements of the
     * vector deep copy under assignment.
     *
     * @tparam T Scalar type.
     * @param v Input vector.
     * @return Deep copy of input.
     */
    template <typename T>
    inline std::vector<T> deep_copy(const std::vector<T>& v) {
      std::vector<T> result(v);
      return result;
    }

  }
}
#endif
