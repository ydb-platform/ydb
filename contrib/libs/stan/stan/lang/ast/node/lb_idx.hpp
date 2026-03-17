#ifndef STAN_LANG_AST_NODE_LB_IDX_HPP
#define STAN_LANG_AST_NODE_LB_IDX_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST structure for holding a lower-bound index.
     */
    struct lb_idx {
      /**
       * Lower bound.
       */
      expression lb_;

      /**
       * Construct a default lower-bound index (nil valued).
       */
      lb_idx();

      /**
       * Construct a lower-bound index with specified lower bound.
       *
       * @param lb lower bound
       */
      lb_idx(const expression& lb);  // NOLINT(runtime/explicit)
    };

  }
}
#endif
