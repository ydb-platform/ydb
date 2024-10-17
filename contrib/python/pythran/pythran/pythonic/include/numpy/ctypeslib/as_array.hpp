#ifndef PYTHONIC_INCLUDE_NUMPY_CTYPESLIB_AS_ARRAY_HPP
#define PYTHONIC_INCLUDE_NUMPY_CTYPESLIB_AS_ARRAY_HPP

#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/types/pointer.hpp"

PYTHONIC_NS_BEGIN
namespace numpy
{
  namespace ctypeslib
  {
    template <class T, class pS>
    typename std::enable_if<!std::is_integral<pS>::value,
                            types::ndarray<T, pS>>::type
        as_array(types::pointer<T>, pS);

    template <class T>
    types::ndarray<T, types::pshape<long>> as_array(types::pointer<T>, long);
    DEFINE_FUNCTOR(pythonic::numpy::ctypeslib, as_array);
  }
}
PYTHONIC_NS_END

#endif
