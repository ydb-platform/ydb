#ifndef STAN_LANG_AST_FUN_IS_SPACE_DEF_HPP
#define STAN_LANG_AST_FUN_IS_SPACE_DEF_HPP

#include <stan/io/is_whitespace.hpp>
#include <stan/lang/ast/fun/is_space.hpp>

namespace stan {
  namespace lang {

    bool is_space(char c) {
      return stan::io::is_whitespace(c);
    }

  }
}
#endif
