#ifndef STAN_LANG_AST_FUN_HAS_PROB_FUN_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_PROB_FUN_SUFFIX_DEF_HPP

#include <stan/lang/ast/fun/ends_with.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool has_prob_fun_suffix(const std::string& fname) {
      return ends_with("_lpdf", fname) || ends_with("_lpmf", fname)
        || ends_with("_log", fname);
    }

  }
}
#endif
