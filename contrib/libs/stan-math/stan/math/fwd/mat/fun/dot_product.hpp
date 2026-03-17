#ifndef STAN_MATH_FWD_MAT_FUN_DOT_PRODUCT_HPP
#define STAN_MATH_FWD_MAT_FUN_DOT_PRODUCT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                           const Eigen::Matrix<fvar<T>, R2, C2>& v2) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < v1.size(); i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                           const Eigen::Matrix<double, R2, C2>& v2) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < v1.size(); i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<double, R1, C1>& v1,
                           const Eigen::Matrix<fvar<T>, R2, C2>& v2) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < v1.size(); i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                           const Eigen::Matrix<fvar<T>, R2, C2>& v2,
                           size_type& length) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<fvar<T>, R1, C1>& v1,
                           const Eigen::Matrix<double, R2, C2>& v2,
                           size_type& length) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T, int R1, int C1, int R2, int C2>
inline fvar<T> dot_product(const Eigen::Matrix<double, R1, C1>& v1,
                           const Eigen::Matrix<fvar<T>, R2, C2>& v2,
                           size_type& length) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);

  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1(i) * v2(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<fvar<T> >& v1,
                           const std::vector<fvar<T> >& v2) {
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  fvar<T> ret(0, 0);
  for (size_t i = 0; i < v1.size(); i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<double>& v1,
                           const std::vector<fvar<T> >& v2) {
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  fvar<T> ret(0, 0);
  for (size_t i = 0; i < v1.size(); i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<fvar<T> >& v1,
                           const std::vector<double>& v2) {
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  fvar<T> ret(0, 0);
  for (size_t i = 0; i < v1.size(); i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<fvar<T> >& v1,
                           const std::vector<fvar<T> >& v2, size_type& length) {
  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<double>& v1,
                           const std::vector<fvar<T> >& v2, size_type& length) {
  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

template <typename T>
inline fvar<T> dot_product(const std::vector<fvar<T> >& v1,
                           const std::vector<double>& v2, size_type& length) {
  fvar<T> ret(0, 0);
  for (size_type i = 0; i < length; i++)
    ret += v1.at(i) * v2.at(i);
  return ret;
}

}  // namespace math
}  // namespace stan
#endif
