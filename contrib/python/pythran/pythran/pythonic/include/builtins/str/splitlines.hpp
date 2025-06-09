#ifndef PYTHONIC_INCLUDE_BUILTIN_STR_SPLITLINES_HPP
#define PYTHONIC_INCLUDE_BUILTIN_STR_SPLITLINES_HPP

#include "pythonic/include/types/list.hpp"
#include "pythonic/include/types/str.hpp"
#include "pythonic/include/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace builtins
{

  namespace str
  {

    types::list<types::str> splitlines(types::str const &in, bool keepends = false);

    DEFINE_FUNCTOR(pythonic::builtins::str, splitlines);
  }
}
PYTHONIC_NS_END
#endif
