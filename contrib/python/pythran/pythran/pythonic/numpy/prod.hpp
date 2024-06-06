#ifndef PYTHONIC_NUMPY_PROD_HPP
#define PYTHONIC_NUMPY_PROD_HPP

#include "pythonic/include/numpy/prod.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/numpy/reduce.hpp"
#include "pythonic/operator_/imul.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  template <class... Args>
  auto prod(Args &&... args)
      -> decltype(reduce<operator_::functor::imul>(std::forward<Args>(args)...))
  {
    return reduce<operator_::functor::imul>(std::forward<Args>(args)...);
  }
}
PYTHONIC_NS_END

#endif
