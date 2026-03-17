#ifndef STAN_LANG_AST_NODE_WHILE_STATEMENT_HPP
#define STAN_LANG_AST_NODE_WHILE_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/statement.hpp>


namespace stan {
  namespace lang {

    /**
     * AST node for representing while statements.
     */
    struct while_statement {
      /**
       * Construct an unitialized while statement with nil condition
       * and body. 
       */
      while_statement();

      /**
       * Construct a while statement with the specified loop condition
       * and loop body.
       *
       * @param[in] condition loop condition
       * @param[in] body loop body
       */
      while_statement(const expression& condition, const statement& body);

      /**
       * The loop condition.
       */
      expression condition_;

      /**
       * The loop body.
       */
      statement body_;
    };

  }
}
#endif
