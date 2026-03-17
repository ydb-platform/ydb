#ifndef STAN_LANG_AST_NODE_UB_IDX_HPP
#define STAN_LANG_AST_NODE_UB_IDX_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST structure for holding an upper-bound index.
     */
    struct ub_idx {
      /**
       * Upper bound.
       */
      expression ub_;

      /**
       * Construct a default (nil valued) upper-bound index.
       */
      ub_idx();

      /**
       * Construct an upper-bound index with specified bound.
       *
       * @param ub upper bound
       */
      ub_idx(const expression& ub);  // NOLINT(runtime/explicit)
    };

  }
}
#endif
