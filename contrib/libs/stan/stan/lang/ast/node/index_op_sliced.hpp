#ifndef STAN_LANG_AST_NODE_INDEX_OP_SLICED_HPP
#define STAN_LANG_AST_NODE_INDEX_OP_SLICED_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/idx.hpp>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST structure for holding an expression with a sequence of
     * indexes.
     */
    struct index_op_sliced {
      /**
       * Expression being indexed.
       */
      expression expr_;

      /**
       * Sequence of indexes.
       */
      std::vector<idx> idxs_;

      /**
       * Type of result.
       */
      bare_expr_type type_;

      /**
       * Construct a default indexed expression (all nil).
       */
      index_op_sliced();

      /**
       * Construct an indexed expression from the specified expression
       * and indexes.
       *
       * @param expr expression being indexed
       * @param idxs indexes
       */
      index_op_sliced(const expression& expr,
                      const std::vector<idx>& idxs);

      /**
       * Infer the type of the result.  Modifies the underlying
       * expression type and not well formed until this is run.
       */
      void infer_type();
    };

  }
}
#endif
