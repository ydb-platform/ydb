#ifndef STAN_LANG_AST_NODE_FOR_ARRAY_STATEMENT_HPP
#define STAN_LANG_AST_NODE_FOR_ARRAY_STATEMENT_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <string>

namespace stan {
  namespace lang {

    /**
     * AST node for representing a foreach statement over an array.
     */
    struct for_array_statement {
      /**
       * Construct an uninitialized foreach statement.
       */
      for_array_statement();

      /**
       * Construct a foreach statement that loops the specified variable
       * over the specified expression to execute the specified statement.
       *
       * @param[in] variable loop variable
       * @param[in] expression value expression foreach loop variable
       * @param[in] stmt body of the foreach loop
       */
      for_array_statement(const std::string& variable,
                          const expression& expression,
                          const statement& stmt);

      /**
       * The loop variable.
       */
      std::string variable_;

      /**
       * The expression of values for the loop variable.
       */
      expression expression_;

      /**
       * The body of the foreach loop.
       */
      statement statement_;
    };

  }
}
#endif
