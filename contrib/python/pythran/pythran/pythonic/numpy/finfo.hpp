#ifndef PYTHONIC_NUMPY_FINFO_HPP
#define PYTHONIC_NUMPY_FINFO_HPP

#include "pythonic/include/numpy/finfo.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/finfo.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  template <class dtype>
  types::finfo<typename dtype::type> finfo(dtype d)
  {
    return types::finfo<typename dtype::type>();
  }
}
PYTHONIC_NS_END

#endif
