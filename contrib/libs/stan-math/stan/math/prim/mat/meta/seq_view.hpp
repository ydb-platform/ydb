#ifndef STAN_MATH_PRIM_MAT_META_SEQ_VIEW_HPP
#define STAN_MATH_PRIM_MAT_META_SEQ_VIEW_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T>
struct store_type {
  typedef const T& type;
};
template <>
struct store_type<double> {
  typedef const double type;
};
template <>
struct store_type<int> {
  typedef const int type;
};

template <typename T>
struct pass_type {
  typedef const T& type;
};
template <>
struct pass_type<double> {
  typedef double type;
};
template <>
struct pass_type<int> {
  typedef int type;
};

// S assignable to T
template <typename T, typename S>
class seq_view {
 private:
  typename store_type<S>::type x_;

 public:
  explicit seq_view(typename pass_type<S>::type x) : x_(x) {}
  inline typename pass_type<T>::type operator[](int n) const { return x_; }
  int size() const { return 1; }
};

template <typename T, typename S>
class seq_view<T, Eigen::Matrix<S, Eigen::Dynamic, 1> > {
 private:
  typename store_type<Eigen::Matrix<S, Eigen::Dynamic, 1> >::type x_;

 public:
  explicit seq_view(
      typename pass_type<Eigen::Matrix<S, Eigen::Dynamic, 1> >::type x)
      : x_(x) {}
  inline typename pass_type<T>::type operator[](int n) const { return x_(n); }
  int size() const { return x_.size(); }
};

template <typename T, typename S>
class seq_view<T, Eigen::Matrix<S, 1, Eigen::Dynamic> > {
 private:
  typename store_type<Eigen::Matrix<S, 1, Eigen::Dynamic> >::type x_;

 public:
  explicit seq_view(
      typename pass_type<Eigen::Matrix<S, 1, Eigen::Dynamic> >::type x)
      : x_(x) {}
  inline typename pass_type<T>::type operator[](int n) const { return x_(n); }
  int size() const { return x_.size(); }
};

// row-major order of returns to match std::vector
template <typename T, typename S>
class seq_view<T, Eigen::Matrix<S, Eigen::Dynamic, Eigen::Dynamic> > {
 private:
  typename store_type<Eigen::Matrix<S, Eigen::Dynamic, Eigen::Dynamic> >::type
      x_;

 public:
  explicit seq_view(typename pass_type<
                    Eigen::Matrix<S, Eigen::Dynamic, Eigen::Dynamic> >::type x)
      : x_(x) {}
  inline typename pass_type<T>::type operator[](int n) const {
    return x_(n / x_.cols(), n % x_.cols());
  }
  int size() const { return x_.size(); }
};

// question is how expensive the ctor is
template <typename T, typename S>
class seq_view<T, std::vector<S> > {
 private:
  typename store_type<std::vector<S> >::type x_;
  const size_t elt_size_;

 public:
  explicit seq_view(typename pass_type<std::vector<S> >::type x)
      : x_(x), elt_size_(x_.size() == 0 ? 0 : seq_view<T, S>(x_[0]).size()) {}
  inline typename pass_type<T>::type operator[](int n) const {
    return seq_view<T, S>(x_[n / elt_size_])[n % elt_size_];
  }
  int size() const { return x_.size() * elt_size_; }
};

// BELOW HERE JUST FOR EFFICIENCY

template <typename T>
class seq_view<T, std::vector<T> > {
 private:
  typename store_type<std::vector<T> >::type x_;

 public:
  explicit seq_view(typename pass_type<std::vector<T> >::type x) : x_(x) {}
  inline typename pass_type<T>::type operator[](int n) const { return x_[n]; }
  int size() const { return x_.size(); }
};

// if vector of S with S assignable to T, also works
// use enable_if?  (and disable_if for the general case)
template <typename T>
class seq_view<T, std::vector<std::vector<T> > > {
 private:
  typename store_type<std::vector<std::vector<T> > >::type x_;
  const size_t cols_;

 public:
  explicit seq_view(typename pass_type<std::vector<std::vector<T> > >::type x)
      : x_(x), cols_(x_.size() == 0 ? 0 : x_[0].size()) {}
  inline typename pass_type<T>::type operator[](int n) const {
    return x_[n / cols_][n % cols_];
  }
  int size() const { return x_.size() * cols_; }
};

template <>
class seq_view<double, std::vector<int> > {
 private:
  store_type<std::vector<int> >::type x_;

 public:
  explicit seq_view(pass_type<std::vector<int> >::type x) : x_(x) {}
  inline pass_type<double>::type operator[](int n) const { return x_[n]; }
  int size() const { return x_.size(); }
};

}  // namespace math
}  // namespace stan
#endif
