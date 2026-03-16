#ifndef STAN_LANG_AST_NODE_BREAK_CONTINUE_STATEMENT_HPP
#define STAN_LANG_AST_NODE_BREAK_CONTINUE_STATEMENT_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * AST structure for break and continue statements.
     */
    struct break_continue_statement {
      /**
       * Construct an uninitialized break or continue statement.
       */
      break_continue_statement();

      /**
       * Construct a break or continue statement that generates the
       * specified string.
       *
       * @param generate "break" or "continue"
       */
      explicit break_continue_statement(const std::string& generate);

      /**
       * Text to generate, "break" or "continue".
       */
      std::string generate_;
    };

  }
}
#endif
