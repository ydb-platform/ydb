#ifndef STAN_LANG_AST_NODE_INCREMENT_LOG_PROB_STATEMENT_HPP
#define STAN_LANG_AST_NODE_INCREMENT_LOG_PROB_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST node for the increment log prob (deprecated) and target
     * increment statements.
     */
    struct increment_log_prob_statement {
      /**
       * Construct an increment log prob statement with a nil return
       * expression. 
       */
      increment_log_prob_statement();

      /**
       * Construct an increment log prob statement with the specified
       * expression for the quantity to increment.
       *
       * @param log_prob quantity with which to increment the target
       * log density
       */
      increment_log_prob_statement(const expression& log_prob);  // NOLINT

      /**
       * Expression for the quantity with which to increment the
       * target log density.
       */
      expression log_prob_;
    };

  }
}
#endif
