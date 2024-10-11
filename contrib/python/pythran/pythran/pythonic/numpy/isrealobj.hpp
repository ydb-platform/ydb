#ifndef PYTHONIC_NUMPY_ISREALOBJ_HPP
#define PYTHONIC_NUMPY_ISREALOBJ_HPP

#include "pythonic/include/numpy/isrealobj.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  template <class E>
  constexpr bool isrealobj(E const &expr)
  {
    return !types::is_complex<typename E::dtype>::value;
  }
}
PYTHONIC_NS_END

#endif
