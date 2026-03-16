#ifndef STAN_LANG_AST_NODE_RETURN_STATEMENT_HPP
#define STAN_LANG_AST_NODE_RETURN_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST node for the return statement.
     */
    struct return_statement {
      /**
       * Construct a return statement with a nil return value.
       */
      return_statement();

      /**
       * Construct a return statement with the specified return value.
       *
       * @param[in] expr return value
       */
      return_statement(const expression& expr);  // NOLINT(runtime/explicit)

      /**
       * The value returned.
       */
      expression return_value_;
    };

  }
}
#endif
