#ifndef STAN_IO_WRITER_HPP
#define STAN_IO_WRITER_HPP

#include <stan/math/prim/mat.hpp>
#include <stdexcept>
#include <vector>

namespace stan {

namespace io {

/**
 * A stream-based writer for integer, scalar, vector, matrix
 * and array data types, which transforms from constrained to
 * a sequence of constrained variables.
 *
 * <p>This class converts constrained values to unconstrained
 * values with mappings that invert those defined in
 * <code>stan::io::reader</code> to convert unconstrained values
 * to constrained values.
 *
 * @tparam T Basic scalar type.
 */
template <typename T> class writer {
private:
  std::vector<T> data_r_;
  std::vector<int> data_i_;

public:
  typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> matrix_t;
  typedef Eigen::Matrix<T, Eigen::Dynamic, 1> vector_t;
  typedef Eigen::Matrix<T, 1, Eigen::Dynamic> row_vector_t;

  typedef Eigen::Array<T, Eigen::Dynamic, 1> array_vec_t;

  /**
   * This is the tolerance for checking arithmetic bounds
   * in rank and in simplexes.  The current value is <code>1E-8</code>.
   */
  const double CONSTRAINT_TOLERANCE;

  /**
   * Construct a writer that writes to the specified
   * scalar and integer vectors.
   *
   * @param data_r Scalar values.
   * @param data_i Integer values.
   */
  writer(std::vector<T> &data_r, std::vector<int> &data_i)
      : data_r_(data_r), data_i_(data_i), CONSTRAINT_TOLERANCE(1E-8) {
    data_r_.clear();
    data_i_.clear();
  }

  /**
   * Destroy this writer.
   */
  ~writer() {}

  /**
   * Return a reference to the underlying vector of real values
   * that have been written.
   *
   * @return Values that have been written.
   */
  std::vector<T> &data_r() { return data_r_; }

  /**
   * Return a reference to the underlying vector of integer values
   * that have been written.
   *
   * @return Values that have been written.
   */
  std::vector<int> &data_i() { return data_i_; }

  /**
   * Write the specified integer to the sequence of integer values.
   *
   * @param n Integer to write.
   */
  void integer(int n) { data_i_.push_back(n); }

  /**
   * Write the unconstrained value corresponding to the specified
   * scalar.  Here, the unconstrain operation is a no-op, which
   * matches <code>reader::scalar_constrain()</code>.
   *
   * @param y The value.
   */
  void scalar_unconstrain(T &y) { data_r_.push_back(y); }

  /**
   * Write the unconstrained value corresponding to the specified
   * positive-constrained scalar.  The transformation applied is
   * <code>log(y)</code>, which is the inverse of constraining
   * transform specified in <code>reader::scalar_pos_constrain()</code>.
   *
   * <p>This method will fail if the argument is not non-negative.
   *
   * @param y The positive value.
   * @throw std::runtime_error if y is negative.
   */
  void scalar_pos_unconstrain(T &y) {
    if (y < 0.0)
      BOOST_THROW_EXCEPTION(std::runtime_error("y is negative"));
    data_r_.push_back(log(y));
  }

  /**
   * Return the unconstrained version of the specified input,
   * which is constrained to be above the specified lower bound.
   * The unconstraining transform is <code>log(y - lb)</code>, which
   * inverts the constraining
   * transform defined in <code>reader::scalar_lb_constrain(double)</code>,
   *
   * @param lb Lower bound.
   * @param y Lower-bounded value.
   * @throw std::runtime_error if y is lower than the lower bound provided.
   */
  void scalar_lb_unconstrain(double lb, T &y) {
    data_r_.push_back(stan::math::lb_free(y, lb));
  }

  /**
   * Write the unconstrained value corresponding to the specified
   * lower-bounded value.  The unconstraining transform is
   * <code>log(ub - y)</code>, which reverses the constraining
   * transform defined in <code>reader::scalar_ub_constrain(double)</code>.
   *
   * @param ub Upper bound.
   * @param y Constrained value.
   * @throw std::runtime_error if y is higher than the upper bound provided.
   */
  void scalar_ub_unconstrain(double ub, T &y) {
    data_r_.push_back(stan::math::ub_free(y, ub));
  }

  /**
   * Write the unconstrained value corresponding to the specified
   * value with the specified bounds.  The unconstraining
   * transform is given by <code>reader::logit((y-L)/(U-L))</code>, which
   * inverts the constraining transform defined in
   * <code>scalar_lub_constrain(double,double)</code>.
   *
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @param y Bounded value.
   * @throw std::runtime_error if y is not between the lower and upper bounds
   */
  void scalar_lub_unconstrain(double lb, double ub, T &y) {
    data_r_.push_back(stan::math::lub_free(y, lb, ub));
  }

  /**
   * Write the unconstrained value corresponding to the specified
   * value with the specified offset and multiplier.  The unconstraining
   * transform is given by <code>(y-offset)/multiplier</code>, which
   * inverts the constraining transform defined in
   * <code>scalar_offset_multiplier_constrain(double,double)</code>.
   *
   * @param offset offset.
   * @param multiplier multiplier.
   * @param y Bounded value.
   */
  void scalar_offset_multiplier_unconstrain(double offset, double multiplier,
                                            T &y) {
    data_r_.push_back(
        stan::math::offset_multiplier_free(y, offset, multiplier));
  }

  /**
   * Write the unconstrained value corresponding to the specified
   * correlation-constrained variable.
   *
   * <p>The unconstraining transform is <code>atanh(y)</code>, which
   * reverses the transfrom in <code>corr_constrain()</code>.
   *
   * @param y Correlation value.
   * @throw std::runtime_error if y is not between -1.0 and 1.0
   */
  void corr_unconstrain(T &y) { data_r_.push_back(stan::math::corr_free(y)); }

  /**
   * Write the unconstrained value corresponding to the
   * specified probability value.
   *
   * <p>The unconstraining transform is <code>logit(y)</code>,
   * which inverts the constraining transform defined in
   * <code>prob_constrain()</code>.
   *
   * @param y Probability value.
   * @throw std::runtime_error if y is not between 0.0 and 1.0
   */
  void prob_unconstrain(T &y) { data_r_.push_back(stan::math::prob_free(y)); }

  /**
   * Write the unconstrained vector that corresponds to the specified
   * ascendingly ordered vector.
   *
   * <p>The unconstraining transform is defined for input vector <code>y</code>
   * to produce an output vector <code>x</code> of the same size, defined
   * by <code>x[0] = log(y[0])</code> and by
   * <code>x[k] = log(y[k] - y[k-1])</code> for <code>k > 0</code>.  This
   * unconstraining transform inverts the constraining transform specified
   * in <code>ordered_constrain(size_t)</code>.
   *
   * @param y Ascendingly ordered vector.
   * @return Unconstrained vector corresponding to the specified vector.
   * @throw std::runtime_error if vector is not in ascending order.
   */
  void ordered_unconstrain(vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    if (y.size() == 0)
      return;
    stan::math::check_ordered("stan::io::ordered_unconstrain", "Vector", y);
    data_r_.push_back(y[0]);
    for (idx_t i = 1; i < y.size(); ++i) {
      data_r_.push_back(log(y[i] - y[i - 1]));
    }
  }

  /**
   * Write the unconstrained vector that corresponds to the specified
   * postiive ascendingly ordered vector.
   *
   * <p>The unconstraining transform is defined for input vector
   * <code>y</code> to produce an output vector <code>x</code> of
   * the same size, defined by <code>x[0] = log(y[0])</code> and
   * by <code>x[k] = log(y[k] - y[k-1])</code> for <code>k >
   * 0</code>.  This unconstraining transform inverts the
   * constraining transform specified in
   * <code>positive_ordered_constrain(size_t)</code>.
   *
   * @param y Positive ascendingly ordered vector.
   * @return Unconstrained vector corresponding to the specified vector.
   * @throw std::runtime_error if vector is not in ascending order.
   */
  void positive_ordered_unconstrain(vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;

    // reimplements pos_ordered_free in prob to avoid malloc
    if (y.size() == 0)
      return;
    stan::math::check_positive_ordered("stan::io::positive_ordered_unconstrain",
                                       "Vector", y);
    data_r_.push_back(log(y[0]));
    for (idx_t i = 1; i < y.size(); ++i) {
      data_r_.push_back(log(y[i] - y[i - 1]));
    }
  }

  /**
   * Write the specified unconstrained vector.
   *
   * @param y Vector to write.
   */
  void vector_unconstrain(const vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      data_r_.push_back(y[i]);
  }

  /**
   * Write the specified unconstrained vector.
   *
   * @param y Vector to write.
   */
  void row_vector_unconstrain(const vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      data_r_.push_back(y[i]);
  }

  /**
   * Write the specified unconstrained matrix.
   *
   * @param y Matrix to write.
   */
  void matrix_unconstrain(const matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    for (idx_t j = 0; j < y.cols(); ++j)
      for (idx_t i = 0; i < y.rows(); ++i)
        data_r_.push_back(y(i, j));
  }

  void vector_lb_unconstrain(double lb, vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_lb_unconstrain(lb, y(i));
  }
  void row_vector_lb_unconstrain(double lb, row_vector_t &y) {
    typedef typename stan::math::index_type<row_vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_lb_unconstrain(lb, y(i));
  }
  void matrix_lb_unconstrain(double lb, matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    for (idx_t j = 0; j < y.cols(); ++j)
      for (idx_t i = 0; i < y.rows(); ++i)
        scalar_lb_unconstrain(lb, y(i, j));
  }

  void vector_ub_unconstrain(double ub, vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_ub_unconstrain(ub, y(i));
  }
  void row_vector_ub_unconstrain(double ub, row_vector_t &y) {
    typedef typename stan::math::index_type<row_vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_ub_unconstrain(ub, y(i));
  }
  void matrix_ub_unconstrain(double ub, matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    for (idx_t j = 0; j < y.cols(); ++j)
      for (idx_t i = 0; i < y.rows(); ++i)
        scalar_ub_unconstrain(ub, y(i, j));
  }

  void vector_lub_unconstrain(double lb, double ub, vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_lub_unconstrain(lb, ub, y(i));
  }
  void row_vector_lub_unconstrain(double lb, double ub, row_vector_t &y) {
    typedef typename stan::math::index_type<row_vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_lub_unconstrain(lb, ub, y(i));
  }
  void matrix_lub_unconstrain(double lb, double ub, matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    for (idx_t j = 0; j < y.cols(); ++j)
      for (idx_t i = 0; i < y.rows(); ++i)
        scalar_lub_unconstrain(lb, ub, y(i, j));
  }

  void vector_offset_multiplier_unconstrain(double offset, double multiplier,
                                            vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_offset_multiplier_unconstrain(offset, multiplier, y(i));
  }
  void row_vector_offset_multiplier_unconstrain(double offset,
                                                double multiplier,
                                                row_vector_t &y) {
    typedef typename stan::math::index_type<row_vector_t>::type idx_t;
    for (idx_t i = 0; i < y.size(); ++i)
      scalar_offset_multiplier_unconstrain(offset, multiplier, y(i));
  }
  void matrix_offset_multiplier_unconstrain(double offset, double multiplier,
                                            matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    for (idx_t j = 0; j < y.cols(); ++j)
      for (idx_t i = 0; i < y.rows(); ++i)
        scalar_offset_multiplier_unconstrain(offset, multiplier, y(i, j));
  }

  /**
   * Write the unconstrained vector corresponding to the specified
   * unit_vector value.  If the specified constrained unit_vector
   * is of size <code>K</code>, the returned unconstrained vector
   * is of size <code>K-1</code>.
   *
   * <p>The transform takes <code>y = y[1],...,y[K]</code> and
   * produces the unconstrained vector. This inverts
   * the constraining transform of
   * <code>unit_vector_constrain(size_t)</code>.
   *
   * @param y Simplex constrained value.
   * @return Unconstrained value.
   * @throw std::runtime_error if the vector is not a unit_vector.
   */
  void unit_vector_unconstrain(vector_t &y) {
    stan::math::check_unit_vector("stan::io::unit_vector_unconstrain", "Vector",
                                  y);
    typedef typename stan::math::index_type<vector_t>::type idx_t;
    vector_t uy = stan::math::unit_vector_free(y);
    for (idx_t i = 0; i < uy.size(); ++i)
      data_r_.push_back(uy[i]);
  }

  /**
   * Write the unconstrained vector corresponding to the specified simplex
   * value.  If the specified constrained simplex is of size <code>K</code>,
   * the returned unconstrained vector is of size <code>K-1</code>.
   *
   * <p>The transform takes <code>y = y[1],...,y[K]</code> and
   * produces the unconstrained vector. This inverts
   * the constraining transform of
   * <code>simplex_constrain(size_t)</code>.
   *
   * @param y Simplex constrained value.
   * @return Unconstrained value.
   * @throw std::runtime_error if the vector is not a simplex.
   */
  void simplex_unconstrain(vector_t &y) {
    typedef typename stan::math::index_type<vector_t>::type idx_t;

    stan::math::check_simplex("stan::io::simplex_unconstrain", "Vector", y);
    vector_t uy = stan::math::simplex_free(y);
    for (idx_t i = 0; i < uy.size(); ++i)
      data_r_.push_back(uy[i]);
  }

  /**
   * Writes the unconstrained Cholesky factor corresponding to the
   * specified constrained matrix.
   *
   * <p>The unconstraining operation is the inverse of the
   * constraining operation in
   * <code>cholesky_factor_cov_constrain(Matrix<T,Dynamic,Dynamic)</code>.
   *
   * @param y Constrained covariance matrix.
   * @throw std::runtime_error if y has no elements or if it is not square
   */
  void cholesky_factor_cov_unconstrain(matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;

    // FIXME:  optimize by unrolling cholesky_factor_free
    Eigen::Matrix<T, Eigen::Dynamic, 1> y_free =
        stan::math::cholesky_factor_free(y);
    for (idx_t i = 0; i < y_free.size(); ++i)
      data_r_.push_back(y_free[i]);
  }

  /**
   * Writes the unconstrained Cholesky factor for a correlation
   * matrix corresponding to the specified constrained matrix.
   *
   * <p>The unconstraining operation is the inverse of the
   * constraining operation in
   * <code>cholesky_factor_corr_constrain(Matrix<T,Dynamic,Dynamic)</code>.
   *
   * @param y Constrained correlation matrix.
   * @throw std::runtime_error if y has no elements or if it is not square
   */
  void cholesky_factor_corr_unconstrain(matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;

    // FIXME:  optimize by unrolling cholesky_factor_free
    Eigen::Matrix<T, Eigen::Dynamic, 1> y_free =
        stan::math::cholesky_corr_free(y);
    for (idx_t i = 0; i < y_free.size(); ++i)
      data_r_.push_back(y_free[i]);
  }

  /**
   * Writes the unconstrained covariance matrix corresponding
   * to the specified constrained correlation matrix.
   *
   * <p>The unconstraining operation is the inverse of the
   * constraining operation in
   * <code>cov_matrix_constrain(Matrix<T,Dynamic,Dynamic)</code>.
   *
   * @param y Constrained covariance matrix.
   * @throw std::runtime_error if y has no elements or if it is not square
   */
  void cov_matrix_unconstrain(matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;
    idx_t k = y.rows();
    if (k == 0 || y.cols() != k)
      BOOST_THROW_EXCEPTION(std::runtime_error("y must have elements and"
                                               " y must be a square matrix"));
    vector_t L_vec = stan::math::cov_matrix_free(y);
    int i = 0;
    for (idx_t m = 0; m < k; ++m) {
      for (idx_t n = 0; n <= m; ++n)
        data_r_.push_back(L_vec.coeff(i++));
    }
  }

  /**
   * Writes the unconstrained correlation matrix corresponding
   * to the specified constrained correlation matrix.
   *
   * <p>The unconstraining operation is the inverse of the
   * constraining operation in
   * <code>corr_matrix_constrain(Matrix<T,Dynamic,Dynamic)</code>.
   *
   * @param y Constrained correlation matrix.
   * @throw std::runtime_error if the correlation matrix has no elements or
   *    is not a square matrix.
   * @throw std::runtime_error if the correlation matrix is non-symmetric,
   *   diagonals not near 1, not positive definite, or any of the
   *   elements nan.
   */
  void corr_matrix_unconstrain(matrix_t &y) {
    typedef typename stan::math::index_type<matrix_t>::type idx_t;

    stan::math::check_corr_matrix("stan::io::corr_matrix_unconstrain", "Matrix",
                                  y);
    idx_t k = y.rows();
    idx_t k_choose_2 = (k * (k - 1)) / 2;
    vector_t cpcs = stan::math::corr_matrix_free(y);
    for (idx_t i = 0; i < k_choose_2; ++i)
      data_r_.push_back(cpcs[i]);
  }
};
}  // namespace io

}  // namespace stan

#endif
