#ifndef STAN_LANG_AST_NODE_INDEX_OP_HPP
#define STAN_LANG_AST_NODE_INDEX_OP_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Structure for an indexed expression.
     */
    struct index_op {
      /**
       * Expression being indexed.
       */
      expression expr_;

      /**
       * Sequence of sequences of indexes.
       */
      std::vector<std::vector<expression> > dimss_;

      /**
       * Type of indexed expression.
       */
      bare_expr_type type_;

      /**
       * Construct a default indexed expression.
       */
      index_op();

      /**
       * Construct an indexed expression with the specified expression
       * and indices.
       *
       * @param expr expression being indexed
       * @param dimss sequence of sequences of expressions
       */
      index_op(const expression& expr,
               const std::vector<std::vector<expression> >& dimss);

      /**
       * Determine indexed expression type given indexes.
       */
      void infer_type();
    };


  }
}
#endif
