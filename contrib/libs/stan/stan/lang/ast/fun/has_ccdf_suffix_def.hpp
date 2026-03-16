#ifndef STAN_LANG_AST_FUN_HAS_CCDF_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_CCDF_SUFFIX_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool has_ccdf_suffix(const std::string& fname) {
      return ends_with("_lccdf", fname) || ends_with("_ccdf_log", fname);
    }

  }
}
#endif
