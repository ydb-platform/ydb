#ifndef STAN_MATH_PRIM_MAT_META_vector_SEQ_VIEW_HPP
#define STAN_MATH_PRIM_MAT_META_vector_SEQ_VIEW_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {

/**
 * This class provides a low-cost wrapper for situations where you either need
 * an Eigen Vector or RowVector or a std::vector of them and you want to be
 * agnostic between those two options. This is similar to scalar_seq_view but
 * instead of being a sequence-like view over a scalar or seq of scalars, it's
 * a sequence-like view over a Vector or seq of Vectors. Notably this version
 * only allows std::vectors as the container type, since we would have
 * difficulty figuring out which contained type was the container otherwise.
 *
 * @tparam T the wrapped type, either a Vector or std::vector of them.
 */
template <typename T>
class vector_seq_view {};

/**
 * This class provides a low-cost wrapper for situations where you either need
 * an Eigen Vector or RowVector or a std::vector of them and you want to be
 * agnostic between those two options. This is similar to scalar_seq_view but
 * instead of being a sequence-like view over a scalar or seq of scalars, it's
 * a sequence-like view over a Vector or seq of Vectors. Notably this version
 * only allows std::vectors as the container type, since we would have
 * difficulty figuring out which contained type was the container otherwise.
 *
 * @tparam S the type inside of the underlying Vector
 */
template <typename S>
class vector_seq_view<Eigen::Matrix<S, Eigen::Dynamic, 1> > {
 public:
  explicit vector_seq_view(const Eigen::Matrix<S, Eigen::Dynamic, 1>& m)
      : m_(m) {}
  int size() const { return 1; }
  Eigen::Matrix<S, Eigen::Dynamic, 1> operator[](int /* i */) const {
    return m_;
  }

 private:
  const Eigen::Matrix<S, Eigen::Dynamic, 1>& m_;
};

/**
 * This class provides a low-cost wrapper for situations where you either need
 * an Eigen Vector or RowVector or a std::vector of them and you want to be
 * agnostic between those two options. This is similar to scalar_seq_view but
 * instead of being a sequence-like view over a scalar or seq of scalars, it's
 * a sequence-like view over a Vector or seq of Vectors. Notably this version
 * only allows std::vectors as the container type, since we would have
 * difficulty figuring out which contained type was the container otherwise.
 *
 * @tparam S the type inside of the underlying Vector
 */
template <typename S>
class vector_seq_view<Eigen::Matrix<S, 1, Eigen::Dynamic> > {
 public:
  explicit vector_seq_view(const Eigen::Matrix<S, 1, Eigen::Dynamic>& m)
      : m_(m) {}
  int size() const { return 1; }
  Eigen::Matrix<S, 1, Eigen::Dynamic> operator[](int /* i */) const {
    return m_;
  }

 private:
  const Eigen::Matrix<S, 1, Eigen::Dynamic>& m_;
};

/**
 * This class provides a low-cost wrapper for situations where you either need
 * an Eigen Vector or RowVector or a std::vector of them and you want to be
 * agnostic between those two options. This is similar to scalar_seq_view but
 * instead of being a sequence-like view over a scalar or seq of scalars, it's
 * a sequence-like view over a Vector or seq of Vectors. Notably this version
 * only allows std::vectors as the container type, since we would have
 * difficulty figuring out which contained type was the container otherwise.
 *
 * @tparam S the type inside of the underlying Vector
 */
template <typename S>
class vector_seq_view<std::vector<Eigen::Matrix<S, Eigen::Dynamic, 1> > > {
 public:
  explicit vector_seq_view(
      const std::vector<Eigen::Matrix<S, Eigen::Dynamic, 1> >& v)
      : v_(v) {}
  int size() const { return v_.size(); }
  Eigen::Matrix<S, Eigen::Dynamic, 1> operator[](int i) const { return v_[i]; }

 private:
  const std::vector<Eigen::Matrix<S, Eigen::Dynamic, 1> >& v_;
};

/**
 * This class provides a low-cost wrapper for situations where you either need
 * an Eigen Vector or RowVector or a std::vector of them and you want to be
 * agnostic between those two options. This is similar to scalar_seq_view but
 * instead of being a sequence-like view over a scalar or seq of scalars, it's
 * a sequence-like view over a Vector or seq of Vectors. Notably this version
 * only allows std::vectors as the container type, since we would have
 * difficulty figuring out which contained type was the container otherwise.
 *
 * @tparam S the type inside of the underlying Vector
 */
template <typename S>
class vector_seq_view<std::vector<Eigen::Matrix<S, 1, Eigen::Dynamic> > > {
 public:
  explicit vector_seq_view(
      const std::vector<Eigen::Matrix<S, 1, Eigen::Dynamic> >& v)
      : v_(v) {}
  int size() const { return v_.size(); }
  Eigen::Matrix<S, 1, Eigen::Dynamic> operator[](int i) const { return v_[i]; }

 private:
  const std::vector<Eigen::Matrix<S, 1, Eigen::Dynamic> >& v_;
};
}  // namespace stan

#endif
