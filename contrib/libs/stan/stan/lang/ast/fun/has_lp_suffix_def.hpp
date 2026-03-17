#ifndef STAN_LANG_AST_FUN_HAS_LP_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_LP_SUFFIX_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool has_lp_suffix(const std::string& s) {
      int n = s.size();
      return n > 3
        && s[n-1] == 'p'
        && s[n-2] == 'l'
        && s[n-3] == '_';
    }

  }
}
#endif
