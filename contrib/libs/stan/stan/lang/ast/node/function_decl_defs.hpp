#ifndef STAN_LANG_AST_NODE_FUNCTION_DECL_DEFS_HPP
#define STAN_LANG_AST_NODE_FUNCTION_DECL_DEFS_HPP

#include <stan/lang/ast/node/function_decl_def.hpp>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for a sequence of function declarations and
     * definitions.
     */
    struct function_decl_defs {
      /**
       * Construct an empty sequence of declarations and definitions.
       */
      function_decl_defs();

      /**
       * Construct a sequence of declarations and definitions from the
       * specified sequence.
       */
      function_decl_defs(
          const std::vector<function_decl_def>& decl_defs);  // NOLINT

      /**
       * Sequence of declarations and definitions.
       */
      std::vector<function_decl_def> decl_defs_;
    };

  }
}
#endif
