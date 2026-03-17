#ifndef STAN_LANG_AST_NODE_BINARY_OP_HPP
#define STAN_LANG_AST_NODE_BINARY_OP_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Node for storing binary operations consisting of an operation
     * and left and right arguments.
     */
    struct binary_op {
      /**
       * String representation of the operation.
       */
      std::string op;

      /**
       * First argument.
       */
      expression left;

      /**
       * Second argument.
       */
      expression right;

      /**
       * Type of result.
       */
      bare_expr_type type_;

      /**
       * Construct a default binary operation.
       */
      binary_op();

      /**
       * Construct a binary operation of the specified operator and
       * arguments. 
       *
       * @param left first argument
       * @param op operator name
       * @param right second argument
       */
      binary_op(const expression& left, const std::string& op,
                const expression& right);
    };

  }
}
#endif
