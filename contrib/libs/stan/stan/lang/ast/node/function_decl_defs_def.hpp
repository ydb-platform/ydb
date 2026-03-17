#ifndef STAN_LANG_AST_NODE_FUNCTION_DECL_DEFS_DEF_HPP
#define STAN_LANG_AST_NODE_FUNCTION_DECL_DEFS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    function_decl_defs::function_decl_defs() { }

    function_decl_defs::function_decl_defs(
                            const std::vector<function_decl_def>& decl_defs)
      : decl_defs_(decl_defs) { }

  }
}
#endif
