#ifndef STAN_LANG_AST_NODE_CONDITIONAL_STATEMENT_HPP
#define STAN_LANG_AST_NODE_CONDITIONAL_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for conditional statements.
     */
    struct conditional_statement {
      /**
       * Construct an empty conditional statement.
       */
      conditional_statement();

      /**
       * Construct a conditional statement with the parallel sequences
       * of conditions and statements.  If there is a default case at
       * the end of the conditional statement without a condition, the
       * statement sequence will be one element longer than the
       * condition sequence.
       *
       * @param[in] conditions conditions for conditional
       * @param[in] statements bodies of conditionals 
       */
      conditional_statement(const std::vector<expression>& conditions,
                            const std::vector<statement>& statements);
      /**
       * The sequence of conditions (parallel with bodies).
       */
      std::vector<expression> conditions_;

      /**
       * The sequence of bodies to execute.  This is the same size or
       * one longer than <code>conditions_</code>.
       */
      std::vector<statement> bodies_;
    };

  }
}
#endif
