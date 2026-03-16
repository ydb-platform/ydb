#ifndef STAN_LANG_AST_FUN_HAS_CDF_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_CDF_SUFFIX_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool has_cdf_suffix(const std::string& fname) {
      return ends_with("_lcdf", fname) || ends_with("_cdf_log", fname);
    }

  }
}
#endif
