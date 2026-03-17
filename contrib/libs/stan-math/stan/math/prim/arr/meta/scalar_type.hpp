#ifndef STAN_MATH_PRIM_ARR_META_SCALAR_TYPE_HPP
#define STAN_MATH_PRIM_ARR_META_SCALAR_TYPE_HPP

#include <stan/math/prim/scal/meta/scalar_type.hpp>
#include <vector>

namespace stan {
template <typename T>
struct scalar_type<std::vector<T> > {
  typedef typename scalar_type<T>::type type;
};

template <typename T>
struct scalar_type<const std::vector<T> > {
  typedef typename scalar_type<T>::type type;
};

template <typename T>
struct scalar_type<std::vector<T>&> {
  typedef typename scalar_type<T>::type type;
};

template <typename T>
struct scalar_type<const std::vector<T>&> {
  typedef typename scalar_type<T>::type type;
};
}  // namespace stan
#endif
