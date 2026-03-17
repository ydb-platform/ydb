#ifndef STAN_LANG_AST_FUN_HAS_RNG_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_RNG_SUFFIX_DEF_HPP

#include <stan/lang/ast/fun/ends_with.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool has_rng_suffix(const std::string& s) {
      int n = s.size();
      return n > 4
        && s[n-1] == 'g'
        && s[n-2] == 'n'
        && s[n-3] == 'r'
        && s[n-4] == '_';
    }

  }
}
#endif
