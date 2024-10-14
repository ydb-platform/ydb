#ifndef PYTHONIC_INCLUDE_DISPATCH_INDEX_HPP
#define PYTHONIC_INCLUDE_DISPATCH_INDEX_HPP

#include "pythonic/utils/functor.hpp"
#include "pythonic/include/operator_/indexOf.hpp"

PYTHONIC_NS_BEGIN

namespace __dispatch__
{
  USING_FUNCTOR(index, pythonic::operator_::functor::indexOf);
}
PYTHONIC_NS_END
#endif
