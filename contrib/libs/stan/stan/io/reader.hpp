#ifndef STAN_IO_READER_HPP
#define STAN_IO_READER_HPP

#include <boost/throw_exception.hpp>
#include <stan/math/prim/mat.hpp>
#include <vector>

namespace stan {

namespace io {

/**
 * A stream-based reader for integer, scalar, vector, matrix
 * and array data types, with Jacobian calculations.
 *
 * The template parameter <code>T</code> represents the type of
 * scalars and the values in vectors and matrices.  The only
 * requirement on the template type <code>T</code> is that a
 * double can be copied into it, as in
 *
 * <code>T t = 0.0;</code>
 *
 * This includes <code>double</code> itself and the reverse-mode
 * algorithmic differentiation class <code>stan::math::var</code>.
 *
 * <p>For transformed values, the scalar type parameter <code>T</code>
 * must support the transforming operations, such as <code>exp(x)</code>
 * for positive-bounded variables.  It must also support equality and
 * inequality tests with <code>double</code> values.
 *
 * @tparam T Basic scalar type.
 */
template <typename T> class reader {
private:
  std::vector<T> &data_r_;
  std::vector<int> &data_i_;
  size_t pos_;
  size_t int_pos_;

  inline T &scalar_ptr() { return data_r_.at(pos_); }

  inline T &scalar_ptr_increment(size_t m) {
    pos_ += m;
    return data_r_.at(pos_ - m);
  }

  inline int &int_ptr() { return data_i_.at(int_pos_); }

  inline int &int_ptr_increment(size_t m) {
    int_pos_ += m;
    return data_i_.at(int_pos_ - m);
  }

public:
  typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> matrix_t;
  typedef Eigen::Matrix<T, Eigen::Dynamic, 1> vector_t;
  typedef Eigen::Matrix<T, 1, Eigen::Dynamic> row_vector_t;

  typedef Eigen::Map<matrix_t> map_matrix_t;
  typedef Eigen::Map<vector_t> map_vector_t;
  typedef Eigen::Map<row_vector_t> map_row_vector_t;

  /**
   * Construct a variable reader using the specified vectors
   * as the source of scalar and integer values for data.  This
   * class holds a reference to the specified data vectors.
   *
   * Attempting to read beyond the end of the data or integer
   * value sequences raises a runtime exception.
   *
   * @param data_r Sequence of scalar values.
   * @param data_i Sequence of integer values.
   */
  reader(std::vector<T> &data_r, std::vector<int> &data_i)
      : data_r_(data_r), data_i_(data_i), pos_(0), int_pos_(0) {}

  /**
   * Destroy this variable reader.
   */
  ~reader() {}

  /**
   * Return the number of scalars remaining to be read.
   *
   * @return Number of scalars left to read.
   */
  inline size_t available() { return data_r_.size() - pos_; }

  /**
   * Return the number of integers remaining to be read.
   *
   * @return Number of integers left to read.
   */
  inline size_t available_i() { return data_i_.size() - int_pos_; }

  /**
   * Return the next integer in the integer sequence.
   *
   * @return Next integer value.
   */
  inline int integer() {
    if (int_pos_ >= data_i_.size())
      BOOST_THROW_EXCEPTION(std::runtime_error("no more integers to read."));
    return data_i_[int_pos_++];
  }

  /**
   * Return the next integer in the integer sequence.
   * This form is a convenience method to make compiling
   * easier; its behavior is the same as <code>int()</code>
   *
   * @return Next integer value.
   */
  inline int integer_constrain() { return integer(); }

  /**
   * Return the next integer in the integer sequence.
   * This form is a convenience method to make compiling
   * easier; its behavior is the same as <code>integer()</code>
   *
   * @return Next integer value.
   */
  inline int integer_constrain(T & /*log_prob*/) { return integer(); }

  /**
   * Return the next scalar in the sequence.
   *
   * @return Next scalar value.
   */
  inline T scalar() {
    if (pos_ >= data_r_.size())
      BOOST_THROW_EXCEPTION(std::runtime_error("no more scalars to read"));
    return data_r_[pos_++];
  }

  /**
   * Return the next scalar.  For arbitrary scalars,
   * constraint is a no-op.
   *
   * @return Next scalar.
   */
  inline T scalar_constrain() { return scalar(); }

  /**
   * Return the next scalar in the sequence, incrementing
   * the specified reference with the log absolute Jacobian determinant.
   *
   * <p>With no transformation, the Jacobian increment is a no-op.
   *
   * <p>See <code>scalar_constrain()</code>.
   *
   * log_prob Reference to log probability variable to increment.
   * @return Next scalar.
   */
  T scalar_constrain(T & /*log_prob*/) { return scalar(); }

  /**
   * Return a standard library vector of the specified
   * dimensionality made up of the next scalars.
   *
   * @param m Size of vector.
   * @return Vector made up of the next scalars.
   */
  inline std::vector<T> std_vector(size_t m) {
    if (m == 0)
      return std::vector<T>();
    std::vector<T> vec;
    T &start = scalar_ptr_increment(m);
    vec.insert(vec.begin(), &start, &scalar_ptr());
    return vec;
  }

  /**
   * Return a column vector of specified dimensionality made up of
   * the next scalars.
   *
   * @param m Number of rows in the vector to read.
   * @return Column vector made up of the next scalars.
   */
  inline vector_t vector(size_t m) {
    if (m == 0)
      return vector_t();
    return map_vector_t(&scalar_ptr_increment(m), m);
  }
  /**
   * Return a column vector of specified dimensionality made up of
   * the next scalars.  The constraint is a no-op.
   *
   * @param m Number of rows in the vector to read.
   * @return Column vector made up of the next scalars.
   */
  inline vector_t vector_constrain(size_t m) {
    if (m == 0)
      return vector_t();
    return map_vector_t(&scalar_ptr_increment(m), m);
  }
  /**
   * Return a column vector of specified dimensionality made up of
   * the next scalars.  The constraint and hence Jacobian are no-ops.
   *
   * @param m Number of rows in the vector to read.
   * lp Log probability to increment.
   * @return Column vector made up of the next scalars.
   */
  inline vector_t vector_constrain(size_t m, T & /*lp*/) {
    if (m == 0)
      return vector_t();
    return map_vector_t(&scalar_ptr_increment(m), m);
  }

  /**
   * Return a row vector of specified dimensionality made up of
   * the next scalars.
   *
   * @param m Number of rows in the vector to read.
   * @return Column vector made up of the next scalars.
   */
  inline row_vector_t row_vector(size_t m) {
    if (m == 0)
      return row_vector_t();
    return map_row_vector_t(&scalar_ptr_increment(m), m);
  }

  /**
   * Return a row vector of specified dimensionality made up of
   * the next scalars.  The constraint is a no-op.
   *
   * @param m Number of rows in the vector to read.
   * @return Column vector made up of the next scalars.
   */
  inline row_vector_t row_vector_constrain(size_t m) {
    if (m == 0)
      return row_vector_t();
    return map_row_vector_t(&scalar_ptr_increment(m), m);
  }

  /**
   * Return a row vector of specified dimensionality made up of
   * the next scalars.  The constraint is a no-op, so the log
   * probability is not incremented.
   *
   * @param m Number of rows in the vector to read.
   * lp Log probability to increment.
   * @return Column vector made up of the next scalars.
   */
  inline row_vector_t row_vector_constrain(size_t m, T & /*lp*/) {
    if (m == 0)
      return row_vector_t();
    return map_row_vector_t(&scalar_ptr_increment(m), m);
  }

  /**
   * Return a matrix of the specified dimensionality made up of
   * the next scalars arranged in column-major order.
   *
   * Row-major reading means that if a matrix of <code>m=2</code>
   * rows and <code>n=3</code> columns is reada and the next
   * scalar values are <code>1,2,3,4,5,6</code>, the result is
   *
   * <pre>
   * a = 1 4
   *     2 5
   *     3 6</pre>
   *
   * @param m Number of rows.
   * @param n Number of columns.
   * @return Eigen::Matrix made up of the next scalars.
   */
  inline matrix_t matrix(size_t m, size_t n) {
    if (m == 0 || n == 0)
      return matrix_t(m, n);
    return map_matrix_t(&scalar_ptr_increment(m * n), m, n);
  }

  /**
   * Return a matrix of the specified dimensionality made up of
   * the next scalars arranged in column-major order.  The
   * constraint is a no-op.  See <code>matrix(size_t,
   * size_t)</code> for more information.
   *
   * @param m Number of rows.
   * @param n Number of columns.
   * @return Matrix made up of the next scalars.
   */
  inline matrix_t matrix_constrain(size_t m, size_t n) {
    if (m == 0 || n == 0)
      return matrix_t(m, n);
    return map_matrix_t(&scalar_ptr_increment(m * n), m, n);
  }

  /**
   * Return a matrix of the specified dimensionality made up of
   * the next scalars arranged in column-major order.  The
   * constraint is a no-op, hence the log probability is not
   * incremented.  See <code>matrix(size_t, size_t)</code>
   * for more information.
   *
   * @param m Number of rows.
   * @param n Number of columns.
   * lp Log probability to increment.
   * @return Matrix made up of the next scalars.
   */
  inline matrix_t matrix_constrain(size_t m, size_t n, T & /*lp*/) {
    if (m == 0 || n == 0)
      return matrix_t(m, n);
    return map_matrix_t(&scalar_ptr_increment(m * n), m, n);
  }

  /**
   * Return the next integer, checking that it is greater than
   * or equal to the specified lower bound.
   *
   * @param lb Lower bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * greater than or equal to the lower bound.
   */
  inline int integer_lb(int lb) {
    int i = integer();
    if (!(i >= lb))
      BOOST_THROW_EXCEPTION(
          std::runtime_error("required value greater than or equal to lb"));
    return i;
  }
  /**
   * Return the next integer, checking that it is greater than
   * or equal to the specified lower bound.
   *
   * @param lb Lower bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * greater than or equal to the lower bound.
   */
  inline int integer_lb_constrain(int lb) { return integer_lb(lb); }
  /**
   * Return the next integer, checking that it is greater than
   * or equal to the specified lower bound.
   *
   * @param lb Lower bound.
   * lp Log probability (ignored because no Jacobian)
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * greater than or equal to the lower bound.
   */
  inline int integer_lb_constrain(int lb, T & /*lp*/) { return integer_lb(lb); }

  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.
   *
   * @param ub Upper bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  inline int integer_ub(int ub) {
    int i = integer();
    if (!(i <= ub))
      BOOST_THROW_EXCEPTION(
          std::runtime_error("required value less than or equal to ub"));
    return i;
  }
  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.
   *
   * @param ub Upper bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  inline int integer_ub_constrain(int ub) { return integer_ub(ub); }
  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.
   *
   * @param ub Upper bound.
   * lp Log probability (ignored because no Jacobian)
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  int integer_ub_constrain(int ub, T & /*lp*/) { return integer_ub(ub); }

  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.  Even if the upper
   * bounds and lower bounds are not consistent, the next integer
   * value will be consumed.
   *
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  inline int integer_lub(int lb, int ub) {
    // read first to make position deterministic [arbitrary choice]
    int i = integer();
    if (lb > ub)
      BOOST_THROW_EXCEPTION(
          std::runtime_error("lower bound must be less than or equal to ub"));
    if (!(i >= lb))
      BOOST_THROW_EXCEPTION(
          std::runtime_error("required value greater than or equal to lb"));
    if (!(i <= ub))
      BOOST_THROW_EXCEPTION(
          std::runtime_error("required value less than or equal to ub"));
    return i;
  }
  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.
   *
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  inline int integer_lub_constrain(int lb, int ub) {
    return integer_lub(lb, ub);
  }
  /**
   * Return the next integer, checking that it is less than
   * or equal to the specified upper bound.
   *
   * @param lb Lower bound.
   * @param ub Upper bound.
   * lp Log probability (ignored because no Jacobian)
   * @return Next integer read.
   * @throw std::runtime_error If the next integer read is not
   * less than or equal to the upper bound.
   */
  inline int integer_lub_constrain(int lb, int ub, T & /*lp*/) {
    return integer_lub(lb, ub);
  }

  /**
   * Return the next scalar, checking that it is
   * positive.
   *
   * <p>See <code>stan::math::check_positive(T)</code>.
   *
   * @return Next positive scalar.
   * @throw std::runtime_error if x is not positive
   */
  inline T scalar_pos() {
    T x(scalar());
    stan::math::check_positive("stan::io::scalar_pos", "Constrained scalar", x);
    return x;
  }

  /**
   * Return the next scalar, transformed to be positive.
   *
   * <p>See <code>stan::math::positive_constrain(T)</code>.
   *
   * @return The next scalar transformed to be positive.
   */
  inline T scalar_pos_constrain() {
    return stan::math::positive_constrain(scalar());
  }

  /**
   * Return the next scalar transformed to be positive,
   * incrementing the specified reference with the log absolute
   * determinant of the Jacobian.
   *
   * <p>See <code>stan::math::positive_constrain(T,T&)</code>.
   *
   * @param lp Reference to log probability variable to increment.
   * @return The next scalar transformed to be positive.
   */
  inline T scalar_pos_constrain(T &lp) {
    return stan::math::positive_constrain(scalar(), lp);
  }

  /**
   * Return the next scalar, checking that it is
   * greater than or equal to the specified lower bound.
   *
   * <p>See <code>stan::math::check_greater_or_equal(T,double)</code>.
   *
   * @param lb Lower bound.
   * @return Next scalar value.
   * @tparam TL Type of lower bound.
   * @throw std::runtime_error if the scalar is less than the
   *    specified lower bound
   */
  template <typename TL> inline T scalar_lb(const TL lb) {
    T x(scalar());
    stan::math::check_greater_or_equal("stan::io::scalar_lb",
                                       "Constrained scalar", x, lb);
    return x;
  }

  /**
   * Return the next scalar transformed to have the
   * specified lower bound.
   *
   * <p>See <code>stan::math::lb_constrain(T,double)</code>.
   *
   * @tparam TL Type of lower bound.
   * @param lb Lower bound on values.
   * @return Next scalar transformed to have the specified
   * lower bound.
   */
  template <typename TL> inline T scalar_lb_constrain(const TL lb) {
    return stan::math::lb_constrain(scalar(), lb);
  }

  /**
   * Return the next scalar transformed to have the specified
   * lower bound, incrementing the specified reference with the
   * log of the absolute Jacobian determinant of the transform.
   *
   * <p>See <code>stan::math::lb_constrain(T,double,T&)</code>.
   *
   * @tparam TL Type of lower bound.
   * @param lb Lower bound on result.
   * @param lp Reference to log probability variable to increment.
   */
  template <typename TL> inline T scalar_lb_constrain(const TL lb, T &lp) {
    return stan::math::lb_constrain(scalar(), lb, lp);
  }

  /**
   * Return the next scalar, checking that it is
   * less than or equal to the specified upper bound.
   *
   * <p>See <code>stan::math::check_less_or_equal(T,double)</code>.
   *
   * @tparam TU Type of upper bound.
   * @param ub Upper bound.
   * @return Next scalar value.
   * @throw std::runtime_error if the scalar is greater than the
   *    specified upper bound
   */
  template <typename TU> inline T scalar_ub(TU ub) {
    T x(scalar());
    stan::math::check_less_or_equal("stan::io::scalar_ub",
                                    "Constrained scalar",
                                    x, ub);
    return x;
  }

  /**
   * Return the next scalar transformed to have the
   * specified upper bound.
   *
   * <p>See <code>stan::math::ub_constrain(T,double)</code>.
   *
   * @tparam TU Type of upper bound.
   * @param ub Upper bound on values.
   * @return Next scalar transformed to have the specified
   * upper bound.
   */
  template <typename TU> inline T scalar_ub_constrain(const TU ub) {
    return stan::math::ub_constrain(scalar(), ub);
  }

  /**
   * Return the next scalar transformed to have the specified
   * upper bound, incrementing the specified reference with the
   * log of the absolute Jacobian determinant of the transform.
   *
   * <p>See <code>stan::math::ub_constrain(T,double,T&)</code>.
   *
   * @tparam TU Type of upper bound.
   * @param ub Upper bound on result.
   * @param lp Reference to log probability variable to increment.
   */
  template <typename TU> inline T scalar_ub_constrain(const TU ub, T &lp) {
    return stan::math::ub_constrain(scalar(), ub, lp);
  }

  /**
   * Return the next scalar, checking that it is between
   * the specified lower and upper bound.
   *
   * <p>See <code>stan::math::check_bounded(T, double, double)</code>.
   *
   * @tparam TL Type of lower bound.
   * @tparam TU Type of upper bound.
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @return Next scalar value.
   * @throw std::runtime_error if the scalar is not between the specified
   *    lower and upper bounds.
   */
  template <typename TL, typename TU>
  inline T scalar_lub(const TL lb, const TU ub) {
    T x(scalar());
    stan::math::check_bounded<T, TL, TU>("stan::io::scalar_lub",
                                         "Constrained scalar", x, lb, ub);
    return x;
  }

  /**
   * Return the next scalar transformed to be between
   * the specified lower and upper bounds.
   *
   * <p>See <code>stan::math::lub_constrain(T, double, double)</code>.
   *
   * @tparam TL Type of lower bound.
   * @tparam TU Type of upper bound.
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @return Next scalar transformed to fall between the specified
   * bounds.
   */
  template <typename TL, typename TU>
  inline T scalar_lub_constrain(const TL lb, const TU ub) {
    return stan::math::lub_constrain(scalar(), lb, ub);
  }

  /**
   * Return the next scalar transformed to be between the
   * the specified lower and upper bounds.
   *
   * <p>See <code>stan::math::lub_constrain(T, double, double, T&)</code>.
   *
   * @param lb Lower bound.
   * @param ub Upper bound.
   * @param lp Reference to log probability variable to increment.
   * @tparam T Type of scalar.
   * @tparam TL Type of lower bound.
   * @tparam TU Type of upper bound.
   */
  template <typename TL, typename TU>
  inline T scalar_lub_constrain(TL lb, TU ub, T &lp) {
    return stan::math::lub_constrain(scalar(), lb, ub, lp);
  }

  /**
   * Return the next scalar.
   *
   * @tparam TL Type of offset.
   * @tparam TS Type of multiplier.
   * @param offset Offset.
   * @param scal Multiplier.
   * @return Next scalar value.
   */
  template <typename TL, typename TS>
  inline T scalar_offset_multiplier(const TL offset, const TS multiplier) {
    T x(scalar());
    return x;
  }

  /**
   * Return the next scalar transformed to have the specified offset and
   * multiplier.
   *
   * <p>See <code>stan::math::offset_multiplier_constrain(T, double,
   * double)</code>.
   *
   * @tparam TL Type of offset.
   * @tparam TS Type of multiplier.
   * @param offset Offset.
   * @param multiplier Multiplier.
   * @return Next scalar transformed to fall between the specified
   * bounds.
   */
  template <typename TL, typename TS>
  inline T scalar_offset_multiplier_constrain(const TL offset,
                                              const TS multiplier) {
    return stan::math::offset_multiplier_constrain(scalar(), offset,
                                                   multiplier);
  }

  /**
   * Return the next scalar transformed to have the specified offset and
   * multiplier.
   *
   * <p>See <code>stan::math::offset_multiplier_constrain(T, double, double,
   * T&)</code>.
   *
   * @param offset Offset.
   * @param multiplier Multiplier.
   * @param lp Reference to log probability variable to increment.
   * @tparam T Type of scalar.
   * @tparam TL Type of offset.
   * @tparam TS Type of multiplier.
   */
  template <typename TL, typename TS>
  inline T scalar_offset_multiplier_constrain(TL offset, TS multiplier, T &lp) {
    return stan::math::offset_multiplier_constrain(scalar(), offset, multiplier,
                                                   lp);
  }

  /**
   * Return the next scalar, checking that it is a valid value for
   * a probability, between 0 (inclusive) and 1 (inclusive).
   *
   * <p>See <code>stan::math::check_bounded(T)</code>.
   *
   * @return Next probability value.
   */
  inline T prob() {
    T x(scalar());
    stan::math::check_bounded<T, double, double>(
        "stan::io::prob", "Constrained probability", x, 0, 1);
    return x;
  }

  /**
   * Return the next scalar transformed to be a probability
   * between 0 and 1.
   *
   * <p>See <code>stan::math::prob_constrain(T)</code>.
   *
   * @return The next scalar transformed to a probability.
   */
  inline T prob_constrain() { return stan::math::prob_constrain(scalar()); }

  /**
   * Return the next scalar transformed to be a probability
   * between 0 and 1, incrementing the specified reference with
   * the log of the absolute Jacobian determinant.
   *
   * <p>See <code>stan::math::prob_constrain(T)</code>.
   *
   * @param lp Reference to log probability variable to increment.
   * @return The next scalar transformed to a probability.
   */
  inline T prob_constrain(T &lp) {
    return stan::math::prob_constrain(scalar(), lp);
  }

  /**
   * Return the next scalar, checking that it is a valid
   * value for a correlation, between -1 (inclusive) and
   * 1 (inclusive).
   *
   * <p>See <code>stan::math::check_bounded(T)</code>.
   *
   * @return Next correlation value.
   * @throw std::runtime_error if the value is not valid
   *   for a correlation
   */
  inline T corr() {
    T x(scalar());
    stan::math::check_bounded<T, double, double>("stan::io::corr",
                                                 "Correlation value", x, -1, 1);
    return x;
  }

  /**
   * Return the next scalar transformed to be a correlation
   * between -1 and 1.
   *
   * <p>See <code>stan::math::corr_constrain(T)</code>.
   *
   * @return The next scalar transformed to a correlation.
   */
  inline T corr_constrain() { return stan::math::corr_constrain(scalar()); }

  /**
   * Return the next scalar transformed to be a (partial)
   * correlation between -1 and 1, incrementing the specified
   * reference with the log of the absolute Jacobian determinant.
   *
   * <p>See <code>stan::math::corr_constrain(T,T&)</code>.
   *
   * @param lp The reference to the variable holding the log
   * probability to increment.
   * @return The next scalar transformed to a correlation.
   */
  inline T corr_constrain(T &lp) {
    return stan::math::corr_constrain(scalar(), lp);
  }

  /**
   * Return a unit_vector of the specified size made up of the
   * next scalars.
   *
   * <p>See <code>stan::math::check_unit_vector</code>.
   *
   * @param k Size of returned unit_vector.
   * @return unit_vector read from the specified size number of scalars.
   * @throw std::runtime_error if the k values is not a unit_vector.
   */
  inline vector_t unit_vector(size_t k) {
    vector_t theta(vector(k));
    stan::math::check_unit_vector("stan::io::unit_vector", "Constrained vector",
                                  theta);
    return theta;
  }

  /**
   * Return the next unit_vector transformed vector of the specified
   * length.  This operation consumes one less than the specified
   * length number of scalars.
   *
   * <p>See <code>stan::math::unit_vector_constrain(Eigen::Matrix)</code>.
   *
   * @param k Number of dimensions in resulting unit_vector.
   * @return unit_vector derived from next <code>k</code> scalars.
   */
  inline Eigen::Matrix<T, Eigen::Dynamic, 1> unit_vector_constrain(size_t k) {
    return stan::math::unit_vector_constrain(vector(k));
  }

  /**
   * Return the next unit_vector of the specified size (using one fewer
   * unconstrained scalars), incrementing the specified reference with the
   * log absolute Jacobian determinant.
   *
   * <p>See <code>stan::math::unit_vector_constrain(Eigen::Matrix,T&)</code>.
   *
   * @param k Size of unit_vector.
   * @param lp Log probability to increment with log absolute
   * Jacobian determinant.
   * @return The next unit_vector of the specified size.
   */
  inline vector_t unit_vector_constrain(size_t k, T &lp) {
    return stan::math::unit_vector_constrain(vector(k), lp);
  }

  /**
   * Return a simplex of the specified size made up of the
   * next scalars.
   *
   * <p>See <code>stan::math::check_simplex</code>.
   *
   * @param k Size of returned simplex.
   * @return Simplex read from the specified size number of scalars.
   * @throw std::runtime_error if the k values is not a simplex.
   */
  inline vector_t simplex(size_t k) {
    vector_t theta(vector(k));
    stan::math::check_simplex("stan::io::simplex", "Constrained vector", theta);
    return theta;
  }

  /**
   * Return the next simplex transformed vector of the specified
   * length.  This operation consumes one less than the specified
   * length number of scalars.
   *
   * <p>See <code>stan::math::simplex_constrain(Eigen::Matrix)</code>.
   *
   * @param k Number of dimensions in resulting simplex.
   * @return Simplex derived from next <code>k-1</code> scalars.
   */
  inline Eigen::Matrix<T, Eigen::Dynamic, 1> simplex_constrain(size_t k) {
    return stan::math::simplex_constrain(vector(k - 1));
  }

  /**
   * Return the next simplex of the specified size (using one fewer
   * unconstrained scalars), incrementing the specified reference with the
   * log absolute Jacobian determinant.
   *
   * <p>See <code>stan::math::simplex_constrain(Eigen::Matrix,T&)</code>.
   *
   * @param k Size of simplex.
   * @param lp Log probability to increment with log absolute
   * Jacobian determinant.
   * @return The next simplex of the specified size.
   */
  inline vector_t simplex_constrain(size_t k, T &lp) {
    return stan::math::simplex_constrain(vector(k - 1), lp);
  }

  /**
   * Return the next vector of specified size containing
   * values in ascending order.
   *
   * <p>See <code>stan::math::check_ordered(T)</code> for
   * behavior on failure.
   *
   * @param k Size of returned vector.
   * @return Vector of positive values in ascending order.
   */
  inline vector_t ordered(size_t k) {
    vector_t x(vector(k));
    stan::math::check_ordered("stan::io::ordered", "Constrained vector", x);
    return x;
  }

  /**
   * Return the next ordered vector of the specified length.
   *
   * <p>See <code>stan::math::ordered_constrain(Matrix)</code>.
   *
   * @param k Length of returned vector.
   * @return Next ordered vector of the specified
   * length.
   */
  inline vector_t ordered_constrain(size_t k) {
    return stan::math::ordered_constrain(vector(k));
  }

  /**
   * Return the next ordered vector of the specified
   * size, incrementing the specified reference with the log
   * absolute Jacobian of the determinant.
   *
   * <p>See <code>stan::math::ordered_constrain(Matrix,T&)</code>.
   *
   * @param k Size of vector.
   * @param lp Log probability reference to increment.
   * @return Next ordered vector of the specified size.
   */
  inline vector_t ordered_constrain(size_t k, T &lp) {
    return stan::math::ordered_constrain(vector(k), lp);
  }

  /**
   * Return the next vector of specified size containing
   * positive values in ascending order.
   *
   * <p>See <code>stan::math::check_positive_ordered(T)</code> for
   * behavior on failure.
   *
   * @param k Size of returned vector.
   * @return Vector of positive values in ascending order.
   */
  inline vector_t positive_ordered(size_t k) {
    vector_t x(vector(k));
    stan::math::check_positive_ordered("stan::io::positive_ordered",
                                       "Constrained vector", x);
    return x;
  }

  /**
   * Return the next positive ordered vector of the specified length.
   *
   * <p>See <code>stan::math::positive_ordered_constrain(Matrix)</code>.
   *
   * @param k Length of returned vector.
   * @return Next positive_ordered vector of the specified
   * length.
   */
  inline vector_t positive_ordered_constrain(size_t k) {
    return stan::math::positive_ordered_constrain(vector(k));
  }

  /**
   * Return the next positive_ordered vector of the specified
   * size, incrementing the specified reference with the log
   * absolute Jacobian of the determinant.
   *
   * <p>See <code>stan::math::positive_ordered_constrain(Matrix,T&)</code>.
   *
   * @param k Size of vector.
   * @param lp Log probability reference to increment.
   * @return Next positive_ordered vector of the specified size.
   */
  inline vector_t positive_ordered_constrain(size_t k, T &lp) {
    return stan::math::positive_ordered_constrain(vector(k), lp);
  }

  /**
   * Return the next Cholesky factor with the specified
   * dimensionality, reading it directly without transforms.
   *
   * @param M Rows of Cholesky factor
   * @param N Columns of Cholesky factor
   * @return Next Cholesky factor.
   * @throw std::domain_error if the matrix is not a valid
   * Cholesky factor.
   */
  inline matrix_t cholesky_factor_cov(size_t M, size_t N) {
    matrix_t y(matrix(M, N));
    stan::math::check_cholesky_factor("stan::io::cholesky_factor_cov",
                                      "Constrained matrix", y);
    return y;
  }

  /**
   * Return the next Cholesky factor with the specified
   * dimensionality, reading from an unconstrained vector of the
   * appropriate size.
   *
   * @param M Rows of Cholesky factor
   * @param N Columns of Cholesky factor
   * @return Next Cholesky factor.
   * @throw std::domain_error if the matrix is not a valid
   *    Cholesky factor.
   */
  inline matrix_t cholesky_factor_cov_constrain(size_t M, size_t N) {
    return stan::math::cholesky_factor_constrain(
        vector((N * (N + 1)) / 2 + (M - N) * N), M, N);
  }

  /**
   * Return the next Cholesky factor with the specified
   * dimensionality, reading from an unconstrained vector of the
   * appropriate size, and increment the log probability reference
   * with the log Jacobian adjustment for the transform.
   *
   * @param M Rows of Cholesky factor
   * @param N Columns of Cholesky factor
   * @param[in,out] lp log probability
   * @return Next Cholesky factor.
   * @throw std::domain_error if the matrix is not a valid
   *    Cholesky factor.
   */
  inline matrix_t cholesky_factor_cov_constrain(size_t M, size_t N, T &lp) {
    return stan::math::cholesky_factor_constrain(
        vector((N * (N + 1)) / 2 + (M - N) * N), M, N, lp);
  }

  /**
   * Return the next Cholesky factor for a correlation matrix with
   * the specified dimensionality, reading it directly without
   * transforms.
   *
   * @param K Rows and columns of Cholesky factor
   * @return Next Cholesky factor for a correlation matrix.
   * @throw std::domain_error if the matrix is not a valid
   * Cholesky factor for a correlation matrix.
   */
  inline matrix_t cholesky_factor_corr(size_t K) {
    using stan::math::check_cholesky_factor_corr;
    matrix_t y(matrix(K, K));
    check_cholesky_factor_corr("stan::io::cholesky_factor_corr",
                               "Constrained matrix", y);
    return y;
  }

  /**
   * Return the next Cholesky factor for a correlation matrix with
   * the specified dimensionality, reading from an unconstrained
   * vector of the appropriate size.
   *
   * @param K Rows and columns of Cholesky factor.
   * @return Next Cholesky factor for a correlation matrix.
   * @throw std::domain_error if the matrix is not a valid
   *    Cholesky factor for a correlation matrix.
   */
  inline matrix_t cholesky_factor_corr_constrain(size_t K) {
    return stan::math::cholesky_corr_constrain(vector((K * (K - 1)) / 2), K);
  }

  /**
   * Return the next Cholesky factor for a correlation matrix with
   * the specified dimensionality, reading from an unconstrained
   * vector of the appropriate size, and increment the log
   * probability reference with the log Jacobian adjustment for
   * the transform.
   *
   * @param K Rows and columns of Cholesky factor
   * @param lp Log probability reference to increment.
   * @return Next Cholesky factor for a correlation matrix.
   * @throw std::domain_error if the matrix is not a valid
   *    Cholesky factor for a correlation matrix.
   */
  inline matrix_t cholesky_factor_corr_constrain(size_t K, T &lp) {
    return stan::math::cholesky_corr_constrain(vector((K * (K - 1)) / 2), K,
                                               lp);
  }

  /**
   * Return the next covariance matrix with the specified
   * dimensionality.
   *
   * <p>See <code>stan::math::check_cov_matrix(Matrix)</code>.
   *
   * @param k Dimensionality of covariance matrix.
   * @return Next covariance matrix of the specified dimensionality.
   * @throw std::runtime_error if the matrix is not a valid
   *    covariance matrix
   */
  inline matrix_t cov_matrix(size_t k) {
    matrix_t y(matrix(k, k));
    stan::math::check_cov_matrix("stan::io::cov_matrix", "Constrained matrix",
                                 y);
    return y;
  }

  /**
   * Return the next covariance matrix of the specified dimensionality.
   *
   * <p>See <code>stan::math::cov_matrix_constrain(Matrix)</code>.
   *
   * @param k Dimensionality of covariance matrix.
   * @return Next covariance matrix of the specified dimensionality.
   */
  inline matrix_t cov_matrix_constrain(size_t k) {
    return stan::math::cov_matrix_constrain(vector(k + (k * (k - 1)) / 2), k);
  }

  /**
   * Return the next covariance matrix of the specified dimensionality,
   * incrementing the specified reference with the log absolute Jacobian
   * determinant.
   *
   * <p>See <code>stan::math::cov_matrix_constrain(Matrix,T&)</code>.
   *
   * @param k Dimensionality of the (square) covariance matrix.
   * @param lp Log probability reference to increment.
   * @return The next covariance matrix of the specified dimensionality.
   */
  inline matrix_t cov_matrix_constrain(size_t k, T &lp) {
    return stan::math::cov_matrix_constrain(vector(k + (k * (k - 1)) / 2), k,
                                            lp);
  }

  /**
   * Returns the next correlation matrix of the specified dimensionality.
   *
   * <p>See <code>stan::math::check_corr_matrix(Matrix)</code>.
   *
   * @param k Dimensionality of correlation matrix.
   * @return Next correlation matrix of the specified dimensionality.
   * @throw std::runtime_error if the matrix is not a correlation matrix
   */
  inline matrix_t corr_matrix(size_t k) {
    matrix_t x(matrix(k, k));
    stan::math::check_corr_matrix("stan::math::corr_matrix",
                                  "Constrained matrix", x);
    return x;
  }

  /**
   * Return the next correlation matrix of the specified dimensionality.
   *
   * <p>See <code>stan::math::corr_matrix_constrain(Matrix)</code>.
   *
   * @param k Dimensionality of correlation matrix.
   * @return Next correlation matrix of the specified dimensionality.
   */
  inline matrix_t corr_matrix_constrain(size_t k) {
    return stan::math::corr_matrix_constrain(vector((k * (k - 1)) / 2), k);
  }

  /**
   * Return the next correlation matrix of the specified dimensionality,
   * incrementing the specified reference with the log absolute Jacobian
   * determinant.
   *
   * <p>See <code>stan::math::corr_matrix_constrain(Matrix,T&)</code>.
   *
   * @param k Dimensionality of the (square) correlation matrix.
   * @param lp Log probability reference to increment.
   * @return The next correlation matrix of the specified dimensionality.
   */
  inline matrix_t corr_matrix_constrain(size_t k, T &lp) {
    return stan::math::corr_matrix_constrain(vector((k * (k - 1)) / 2), k, lp);
  }

  template <typename TL> inline vector_t vector_lb(const TL lb, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb(lb);
    return v;
  }

  template <typename TL>
  inline vector_t vector_lb_constrain(const TL lb, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb_constrain(lb);
    return v;
  }

  template <typename TL>
  inline vector_t vector_lb_constrain(const TL lb, size_t m, T &lp) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb_constrain(lb, lp);
    return v;
  }

  template <typename TL>
  inline row_vector_t row_vector_lb(const TL lb, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb(lb);
    return v;
  }

  template <typename TL>
  inline row_vector_t row_vector_lb_constrain(const TL lb, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb_constrain(lb);
    return v;
  }

  template <typename TL>
  inline row_vector_t row_vector_lb_constrain(const TL lb, size_t m, T &lp) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lb_constrain(lb, lp);
    return v;
  }

  template <typename TL>
  inline matrix_t matrix_lb(const TL lb, size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lb(lb);
    return v;
  }

  template <typename TL>
  inline matrix_t matrix_lb_constrain(const TL lb, size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lb_constrain(lb);
    return v;
  }

  template <typename TL>
  inline matrix_t matrix_lb_constrain(const TL lb, size_t m, size_t n, T &lp) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lb_constrain(lb, lp);
    return v;
  }

  template <typename TU> inline vector_t vector_ub(const TU ub, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub(ub);
    return v;
  }

  template <typename TU>
  inline vector_t vector_ub_constrain(const TU ub, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub_constrain(ub);
    return v;
  }

  template <typename TU>
  inline vector_t vector_ub_constrain(const TU ub, size_t m, T &lp) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub_constrain(ub, lp);
    return v;
  }

  template <typename TU>
  inline row_vector_t row_vector_ub(const TU ub, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub(ub);
    return v;
  }

  template <typename TU>
  inline row_vector_t row_vector_ub_constrain(const TU ub, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub_constrain(ub);
    return v;
  }

  template <typename TU>
  inline row_vector_t row_vector_ub_constrain(const TU ub, size_t m, T &lp) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_ub_constrain(ub, lp);
    return v;
  }

  template <typename TU>
  inline matrix_t matrix_ub(const TU ub, size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_ub(ub);
    return v;
  }

  template <typename TU>
  inline matrix_t matrix_ub_constrain(const TU ub, size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_ub_constrain(ub);
    return v;
  }

  template <typename TU>
  inline matrix_t matrix_ub_constrain(const TU ub, size_t m, size_t n, T &lp) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_ub_constrain(ub, lp);
    return v;
  }

  template <typename TL, typename TU>
  inline vector_t vector_lub(const TL lb, const TU ub, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub(lb, ub);
    return v;
  }

  template <typename TL, typename TU>
  inline vector_t vector_lub_constrain(const TL lb, const TU ub, size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub_constrain(lb, ub);
    return v;
  }

  template <typename TL, typename TU>
  inline vector_t vector_lub_constrain(const TL lb, const TU ub, size_t m,
                                       T &lp) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub_constrain(lb, ub, lp);
    return v;
  }

  template <typename TL, typename TU>
  inline row_vector_t row_vector_lub(const TL lb, const TU ub, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub(lb, ub);
    return v;
  }
  template <typename TL, typename TU>
  inline row_vector_t row_vector_lub_constrain(const TL lb, const TU ub,
                                               size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub_constrain(lb, ub);
    return v;
  }

  template <typename TL, typename TU>
  inline row_vector_t row_vector_lub_constrain(const TL lb, const TU ub,
                                               size_t m, T &lp) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_lub_constrain(lb, ub, lp);
    return v;
  }

  template <typename TL, typename TU>
  inline matrix_t matrix_lub(const TL lb, const TU ub, size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lub(lb, ub);
    return v;
  }

  template <typename TL, typename TU>
  inline matrix_t matrix_lub_constrain(const TL lb, const TU ub, size_t m,
                                       size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lub_constrain(lb, ub);
    return v;
  }

  template <typename TL, typename TU>
  inline matrix_t matrix_lub_constrain(const TL lb, const TU ub, size_t m,
                                       size_t n, T &lp) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_lub_constrain(lb, ub, lp);
    return v;
  }

  template <typename TL, typename TS>
  inline vector_t vector_offset_multiplier(const TL offset, const TS multiplier,
                                           size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline vector_t vector_offset_multiplier_constrain(const TL offset,
                                                     const TS multiplier,
                                                     size_t m) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier_constrain(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline vector_t vector_offset_multiplier_constrain(const TL offset,
                                                     const TS multiplier,
                                                     size_t m, T &lp) {
    vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier_constrain(offset, multiplier, lp);
    return v;
  }

  template <typename TL, typename TS>
  inline row_vector_t
  row_vector_offset_multiplier(const TL offset, const TS multiplier, size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline row_vector_t
  row_vector_offset_multiplier_constrain(const TL offset, const TS multiplier,
                                         size_t m) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier_constrain(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline row_vector_t
  row_vector_offset_multiplier_constrain(const TL offset, const TS multiplier,
                                         size_t m, T &lp) {
    row_vector_t v(m);
    for (size_t i = 0; i < m; ++i)
      v(i) = scalar_offset_multiplier_constrain(offset, multiplier, lp);
    return v;
  }

  template <typename TL, typename TS>
  inline matrix_t matrix_offset_multiplier(const TL offset, const TS multiplier,
                                           size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_offset_multiplier(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline matrix_t matrix_offset_multiplier_constrain(const TL offset,
                                                     const TS multiplier,
                                                     size_t m, size_t n) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_offset_multiplier_constrain(offset, multiplier);
    return v;
  }

  template <typename TL, typename TS>
  inline matrix_t
  matrix_offset_multiplier_constrain(const TL offset, const TS multiplier,
                                     size_t m, size_t n, T &lp) {
    matrix_t v(m, n);
    for (size_t j = 0; j < n; ++j)
      for (size_t i = 0; i < m; ++i)
        v(i, j) = scalar_offset_multiplier_constrain(offset, multiplier, lp);
    return v;
  }
};

}  // namespace io

}  // namespace stan

#endif
