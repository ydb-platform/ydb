#ifndef STAN_LANG_AST_NODE_MATRIX_EXPR_HPP
#define STAN_LANG_AST_NODE_MATRIX_EXPR_HPP

#include <stan/lang/ast/scope.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <vector>
#include <cstddef>

namespace stan {
  namespace lang {

    struct expresssion;

    /**
     * Structure to hold a matrix expression.
     */
    struct matrix_expr {
      /**
       * Sequence of expressions for matrix values.
       */
      std::vector<expression> args_;

      /**
       * True if there is a variable within any of the expressions
       * that is a parameter, transformed parameter, or non-integer
       * local variable.
       */
      bool has_var_;

      /**
       * Scope of this matrix expression.
       *
       */
      scope matrix_expr_scope_;

      /**
       * Construct a default matrix expression.
       */
      matrix_expr();

      /**
       * Construct an matrix expression from the specified sequence of
       * expressions.
       *
       * @param args sequence of arguments
       */
      explicit matrix_expr(const std::vector<expression>& args);
    };

  }
}
#endif
