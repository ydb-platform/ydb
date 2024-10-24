#ifndef PYTHONIC_INCLUDE_BUILTIN_DICT_COPY_HPP
#define PYTHONIC_INCLUDE_BUILTIN_DICT_COPY_HPP

#include "pythonic/include/__dispatch__/copy.hpp"
#include "pythonic/include/utils/functor.hpp"

PYTHONIC_NS_BEGIN
namespace builtins
{
  namespace dict
  {
    USING_FUNCTOR(copy, pythonic::__dispatch__::functor::copy);
  }
}
PYTHONIC_NS_END

#endif
