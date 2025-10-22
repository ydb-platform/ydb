#ifndef PYTHONIC_BUILTIN_STR_SPLITLINES_HPP
#define PYTHONIC_BUILTIN_STR_SPLITLINES_HPP

#include "pythonic/include/builtins/str/splitlines.hpp"

#include "pythonic/types/list.hpp"
#include "pythonic/types/str.hpp"
#include "pythonic/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace builtins
{

  namespace str
  {

    inline types::list<types::str> splitlines(types::str const &in, bool keepends)
    {
      auto const& in_ = in.chars();
      types::list<types::str> res(0);
      size_t current = 0;
      const ssize_t adjust = keepends ? 1 : 0;
      while (current < (size_t)in.size()) {
        size_t next = in_.find("\n", current);
        res.push_back(in_.substr(current, next - current + adjust));
        current = (next == types::str::npos) ? next : (next + 1);
      }
      return res;
    }

  } // namespace str
} // namespace builtins
PYTHONIC_NS_END
#endif
