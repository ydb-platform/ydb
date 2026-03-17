#ifndef STAN_LANG_AST_NODE_LUB_IDX_HPP
#define STAN_LANG_AST_NODE_LUB_IDX_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST structure for lower and upper bounds.
     */
    struct lub_idx {
      /**
       * Lower bound.
       */
      expression lb_;

      /**
       * Upper bound.
       */
      expression ub_;

      /**
       * Construct a default (nil valued) lower and upper bound index.
       */
      lub_idx();

      /**
       * Construt a lower and upper bound index with the specified
       * bounds.
       *
       * @param lb lower bound
       * @param ub upper bound
       */
      lub_idx(const expression& lb,
              const expression& ub);
    };

  }
}
#endif
