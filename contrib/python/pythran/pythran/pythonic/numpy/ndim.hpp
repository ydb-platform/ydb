#ifndef PYTHONIC_NUMPY_NDIM_HPP
#define PYTHONIC_NUMPY_NDIM_HPP

#include "pythonic/include/numpy/ndim.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  template <class E>
  long ndim(E const &e)
  {
    return std::tuple_size<decltype(shape(e))>::value;
  }
}
PYTHONIC_NS_END

#endif
