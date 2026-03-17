#ifndef STAN_LANG_AST_NODE_REJECT_STATEMENT_HPP
#define STAN_LANG_AST_NODE_REJECT_STATEMENT_HPP

#include <stan/lang/ast/node/printable.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for the reject statement.
     */
    struct reject_statement {
      /**
       * Construct an empty reject statement.
       */
      reject_statement();

      /**
       * Construct a reject statement from the specified sequence of
       * printable objects.
       *
       * @param[in] printables sequence of items to print
       */
      reject_statement(const std::vector<printable>& printables);  // NOLINT

      /**
       * Sequence of objects to print in output message.
       */
      std::vector<printable> printables_;
    };

  }
}
#endif
