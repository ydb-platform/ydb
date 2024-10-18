#ifndef PYTHONIC_INCLUDE_BUILTIN_SET_CLEAR_HPP
#define PYTHONIC_INCLUDE_BUILTIN_SET_CLEAR_HPP

#include "pythonic/include/__dispatch__/clear.hpp"
#include "pythonic/include/utils/functor.hpp"

PYTHONIC_NS_BEGIN
namespace builtins
{
  namespace set
  {
    USING_FUNCTOR(clear, pythonic::__dispatch__::functor::clear);
  }
}
PYTHONIC_NS_END
#endif
