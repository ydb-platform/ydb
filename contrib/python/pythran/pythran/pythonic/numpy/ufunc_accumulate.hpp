#ifndef UFUNC_NAME
#error missing UFUNC_NAME
#endif

// clang-format off
#include INCLUDE_FILE(pythonic/numpy,UFUNC_NAME)
// clang-format on
#include <pythonic/numpy/partial_sum.hpp>
#include "pythonic/utils/functor.hpp"

PYTHONIC_NS_BEGIN
namespace numpy
{
  namespace UFUNC_NAME
  {
    template <class T, class dtype>
    auto accumulate(T &&a, long axis, dtype d)
        -> decltype(partial_sum<numpy::functor::UFUNC_NAME>(std::forward<T>(a),
                                                            axis, d))
    {
      return partial_sum<numpy::functor::UFUNC_NAME>(std::forward<T>(a), axis,
                                                     d);
    }
  }
}
PYTHONIC_NS_END
