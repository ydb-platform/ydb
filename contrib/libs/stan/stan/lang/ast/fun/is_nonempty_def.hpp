#ifndef STAN_LANG_AST_FUN_IS_NONEMPTY_DEF_HPP
#define STAN_LANG_AST_FUN_IS_NONEMPTY_DEF_HPP

#include <stan/lang/ast/fun/is_space.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool is_nonempty(const std::string& s) {
      for (size_t i = 0; i < s.size(); ++i)
        if (!is_space(s[i]))
          return true;
      return false;
    }

  }
}
#endif
