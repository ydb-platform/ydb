#ifndef STAN_MATH_PRIM_MAT_META_IS_VECTOR_HPP
#define STAN_MATH_PRIM_MAT_META_IS_VECTOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/is_vector.hpp>

namespace stan {

// FIXME: use boost::type_traits::remove_all_extents to
//        extend to array/ptr types

template <typename T>
struct is_vector<Eigen::Matrix<T, Eigen::Dynamic, 1> > {
  enum { value = 1 };
  typedef T type;
};
template <typename T>
struct is_vector<Eigen::Matrix<T, 1, Eigen::Dynamic> > {
  enum { value = 1 };
  typedef T type;
};
template <typename T>
struct is_vector<Eigen::Block<T> > {
  enum { value = 1 };
  typedef T type;
};
}  // namespace stan
#endif
