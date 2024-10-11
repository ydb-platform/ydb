#ifndef PYTHONIC_INCLUDE_NUMPY_ARRAYEQUIV_HPP
#define PYTHONIC_INCLUDE_NUMPY_ARRAYEQUIV_HPP

#include "pythonic/include/numpy/array_equal.hpp"
#include "pythonic/include/numpy/asarray.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  template <class U, class V>
  typename std::enable_if<U::value == V::value, bool>::type
  array_equiv(U const &u, V const &v);

  template <class U, class V>
      typename std::enable_if <
      U::value<V::value, bool>::type array_equiv(U const &u, V const &v);

  template <class U, class V>
  typename std::enable_if<(U::value > V::value), bool>::type
  array_equiv(U const &u, V const &v);

  DEFINE_FUNCTOR(pythonic::numpy, array_equiv);
}
PYTHONIC_NS_END

#endif
