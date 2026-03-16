#ifndef STAN_LANG_AST_NODE_PRINT_STATEMENT_HPP
#define STAN_LANG_AST_NODE_PRINT_STATEMENT_HPP

#include <stan/lang/ast/node/printable.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for print statements.
     */
    struct print_statement {
      /**
       * Construct an empty print statement.
       */
      print_statement();

      /**
       * Construct a print statement with the specified sequence of
       * printable objects.
       *
       * @param[in] printables sequence of printable objects
       */
      print_statement(const std::vector<printable>& printables);  // NOLINT

      /**
       * Sequence of printable objects.
       */
      std::vector<printable> printables_;
    };

  }
}
#endif
