#ifndef PYTHONIC_INCLUDE_NUMPY_CROSS_HPP
#define PYTHONIC_INCLUDE_NUMPY_CROSS_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  template <class E, class F>
  types::ndarray<
      typename __combined<typename E::dtype, typename F::dtype>::type,
      types::array<long, E::value>>
  cross(E const &e, F const &f);

  DEFINE_FUNCTOR(pythonic::numpy, cross);
}
PYTHONIC_NS_END

#endif
