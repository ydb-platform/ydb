#ifndef STAN_LANG_AST_NODE_STATEMENTS_HPP
#define STAN_LANG_AST_NODE_STATEMENTS_HPP

#include <vector>

namespace stan {
  namespace lang {

    struct local_var_decl;
    struct statement;

    /**
     * Holder for local variable declarations and a sequence of
     * statements.
     */
    struct statements {
      /**
       * Sequence of variable declarations.
       */
      std::vector<local_var_decl> local_decl_;

      /**
       * Sequence of statements.
       */
      std::vector<statement> statements_;

      /**
       * Nullary constructor for statements.
       */
      statements();

      /**
       * Construct a statements object from a sequence of local
       * declarations and sequence of statements.
       *
       * @param local_decl sequence of local variable declarations
       * @param stmts sequence of statements
       */
      statements(const std::vector<local_var_decl>& local_decl,
                 const std::vector<statement>& stmts);
    };

  }
}
#endif
