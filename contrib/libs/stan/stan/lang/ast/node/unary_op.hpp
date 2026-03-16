#ifndef STAN_LANG_AST_NODE_UNARY_OP_HPP
#define STAN_LANG_AST_NODE_UNARY_OP_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /** 
     * AST structure for unary operations consisting of an operation
     * and argument.
     */
    struct unary_op {
      /**
       * Character-level representation of operation.
       */
      char op;

      /**
       * Argument.
       */
      expression subject;

      /**
       * Type of result.
       */
      bare_expr_type type_;

      /**
       * Construct a default unary operation.
       */
      unary_op();

      /**
       * Construct a unary operation of the specified operation and
       * argument. 
       *
       * @param op operator representation
       * @param subject argument
       */
      unary_op(char op, const expression& subject);
    };

  }
}
#endif
