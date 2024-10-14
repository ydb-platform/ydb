#ifndef PYTHONIC_NUMPY_CTYPESLIB_AS_ARRAY_HPP
#define PYTHONIC_NUMPY_CTYPESLIB_AS_ARRAY_HPP

#include "pythonic/include/numpy/ctypeslib/as_array.hpp"

#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/pointer.hpp"

PYTHONIC_NS_BEGIN
namespace numpy
{
  namespace ctypeslib
  {
    template <class T, class pS>
    typename std::enable_if<!std::is_integral<pS>::value,
                            types::ndarray<T, pS>>::type
    as_array(types::pointer<T> ptr, pS shape)
    {
      return {ptr.data, shape, types::ownership::external};
    }

    template <class T>
    types::ndarray<T, types::pshape<long>> as_array(types::pointer<T> ptr,
                                                    long size)
    {
      return as_array(ptr, types::pshape<long>{size});
    }
  }
}
PYTHONIC_NS_END

#endif
