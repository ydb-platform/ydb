#ifndef STAN_LANG_AST_NODE_FOR_STATEMENT_HPP
#define STAN_LANG_AST_NODE_FOR_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/range.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <string>

namespace stan {
  namespace lang {

    /**
     * AST node for representing a for statement.
     */
    struct for_statement {
      /**
       * Construct an uninitialized for statement.
       */
      for_statement();

      /**
       * Construct a for statement that loops the specified variable
       * over the specified range to execute the specified statement.
       *
       * @param[in] variable loop variable
       * @param[in] range value range for loop variable
       * @param[in] stmt body of the for loop
       */
      for_statement(const std::string& variable, const range& range,
                    const statement& stmt);

      /**
       * The loop variable.
       */
      std::string variable_;

      /**
       * The range of values for the loop variable.
       */
      range range_;

      /**
       * The body of the for loop.
       */
      statement statement_;
    };

  }
}
#endif
